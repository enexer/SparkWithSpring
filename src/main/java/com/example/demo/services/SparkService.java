package com.example.demo.services;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.configuration.PropertiesUtils;
import com.example.demo.configuration.SparkConfiguartion;
import com.example.demo.dto.TaskUrlDto;
import com.example.demo.dto.TasksInfoDto;
import com.example.demo.dto.enums.TaskName;
import com.example.demo.exceptions.SparkMasterUrlException;
import com.example.demo.exceptions.TaskExistException;
import com.example.demo.exceptions.TaskNotFoundException;
import com.example.demo.models.TaskModel;
import io.swagger.models.auth.In;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Int;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Created by as on 07.07.2018.
 */
@Service
public class SparkService {

    private SparkApplicationService sparkApplicationService;
    private SparkConf conf;
    private volatile static Hashtable<UUID, TaskModel> runningTasks = new Hashtable<>();

    @Autowired
    public SparkService(SparkApplicationService sparkApplicationService) {
        this.sparkApplicationService = sparkApplicationService;

        conf = new SparkConf()
                .setAppName("Spark_REST_API_Launcher")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.driver.host", PropertiesModel.spark_driver_host)
                .set("spark.driver.port", PropertiesModel.spark_driver_port) //
                .set("spark.blockManager.port", PropertiesModel.spark_blockManager_port) // Raw socket via ServerSocketChannel
                .set("spark.eventLog.enabled", PropertiesModel.spark_eventLog_enabled)
                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.set("spark.shuffle.service.enabled", "false")
                //.set("spark.dynamicAllocation.enabled", "false")
                //.set("spark.io.compression.codec", "snappy")
                //.set("spark.rdd.compress", "true")
                //.set("spark.executor.cores", "4c")
                //.set("spark.executor.memory", "1g")
                //.set("spark.cores.max", "4")
                //.set("spark.dynamicAllocation.enabled", "false")
                //.setJars(new String[]{PropertiesModel.jars, PropertiesModel.databaseJar})
                .setJars(PropertiesUtils.getJars(PropertiesModel.jars, PropertiesUtils.delimiter))
                .setMaster(PropertiesModel.spark_master);

        System.out.println("SparkService...");
    }

    public JavaSparkContext configureSpark(SparkConf conf) {
        SparkContext context;
        try {
            context = SparkContext.getOrCreate(conf); //new SparkContext(conf);
        } catch (Exception e) {
            throw new SparkMasterUrlException(e.getMessage());
        }
        JavaSparkContext jsc = new JavaSparkContext(context);
        return jsc;
    }

    public List<TaskModel> getAllTasks() {
        List<TaskModel> list = runningTasks.entrySet()
                .stream()
                .map(s -> s.getValue())
                .collect(Collectors.toList());

        return list;
    }

    public static String getProperties() {
        String properties = null;
        try {
            properties = PropertiesModel.printAll();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static String getUrl(HttpServletRequest req) {
        return getBaseUrl(req);
    }

    public static String getBaseUrl(HttpServletRequest req) {
        return req.getScheme() + "://" + req.getServerName() + ":" + req.getServerPort() + req.getContextPath();
    }

    public TasksInfoDto getTasksInfo() {

        int running = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == true)
                .count();

        int finished = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == false)
                .count();

        int runningSparkContexts = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getContext().sc().isStopped() == false)
                .count();

        int total = running + finished;

        return new TasksInfoDto(total, running, finished, runningSparkContexts);
    }

    public String setMaster(String master) {
        conf.setMaster(master);
        return conf.get("spark.master");
    }

    public String getMaster() {
        return conf.get("spark.master");
    }

    /**
     * Method for initialize spark job.
     * Only one job can running at once in JVM instance.
     * When sparkContext object is created then spark job can be executed.
     * To execute multiple spark jobs together, sparkContext should running and spark jobs must use it.
     * When a few spark jobs are running with the same sparkContext, Spark WebUI treat all jobs as stages of main job.
     *
     * @param request
     * @param task
     * @return
     */
    public TaskUrlDto startTask(HttpServletRequest request, TaskName task, String file, String fileDelimiter) {

        conf.setAppName(task.name());
        int maxTasks = SparkConfiguartion.MAX_RUNNING_TASKS; //10;
        boolean stopContextAfterExecution = true; // false for multiple jobs at once.
        int running = (int) runningTasks.entrySet().stream().filter(s -> s.getValue().getRunning().booleanValue() == true).count();
        if (running >= maxTasks) {
            throw new TaskExistException("Cannot start new task, running tasks limit=" + maxTasks);
        }

        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());
        UUID uuid = UUID.randomUUID();
        JavaSparkContext jsc = configureSpark(conf);
        TaskModel taskModel = new TaskModel(true, uuid, time, jsc, task, file, fileDelimiter);
        runningTasks.put(uuid, taskModel);
        new Thread(() -> sparkApplicationService.startTask(uuid, runningTasks, stopContextAfterExecution)).start();
        TaskUrlDto taskUrlDto = new TaskUrlDto();
        taskUrlDto.setTaskUrl(getUrl(request) + "/getresult/" + uuid.toString());
        taskUrlDto.setWebUiUrl(jsc.sc().uiWebUrl().get());
        taskUrlDto.setAppId(jsc.sc().applicationId());
        taskUrlDto.setAppName(jsc.sc().appName());
        return taskUrlDto;
    }

    public TaskModel getTask(String id) {
        if (!Optional.ofNullable(runningTasks.get(UUID.fromString(id))).isPresent()) {
            throw new TaskNotFoundException("Task with id=" + id + " not found.");
        }
        return runningTasks.get(UUID.fromString(id)).setContent("-");
    }

    public String getTaskResult(String id){
        if (!Optional.ofNullable(runningTasks.get(UUID.fromString(id))).isPresent()) {
            throw new TaskNotFoundException("Task with id=" + id + " not found.");
        }
        String content = runningTasks.get(UUID.fromString(id)).getContent();
        String content2 = content.replaceAll("\n","<br>");

        return "<html>\n"+
                "    <body>\n" +
                "         <pre>"+content2+"</pre>\n" +
                "    </body>\n" +
                "</html>\n";
    }

    public TaskModel stopTaskById(String id) {
        runningTasks.get(UUID.fromString(id)).setRunning(false);
        runningTasks.get(UUID.fromString(id)).stopTask();
        return runningTasks.get(UUID.fromString(id));
    }

    public String stopAllTasks() {

        runningTasks.entrySet()
                .stream()
                .forEach(s -> {
                    runningTasks.get(s.getKey()).setRunning(false);
                    runningTasks.get(s.getKey()).stopTask();
                });

        return "stopped all tasks";
    }

    public String clean() {

        int running = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == true)
                .count();

        if (running > 0) {
            throw new TaskExistException("Cannot clean running tasks, stop all tasks before this action.");
        }

        runningTasks.clear();
        return "cleaned all tasks";
    }

}
