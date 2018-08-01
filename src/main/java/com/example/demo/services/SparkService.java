package com.example.demo.services;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.configuration.PropertiesUtils;
import com.example.demo.configuration.SparkConfiguartion;
import com.example.demo.dto.TaskUrlDto;
import com.example.demo.dto.TasksInfoDto;
import com.example.demo.exceptions.SparkMasterUrlException;
import com.example.demo.exceptions.TaskExistException;
import com.example.demo.exceptions.TaskNotFoundException;
import com.example.demo.models.TaskModel;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
    //private SparkContext context;

    @Autowired
    public SparkService(SparkApplicationService sparkApplicationService) {
        this.sparkApplicationService = sparkApplicationService;

        conf = new SparkConf()
                .setAppName("Apache_Spark_ApplicationOK")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.executor.memory", "1g")
                //.set("spark.submit.deployMode", "cluster") // startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.
                .set("spark.driver.host", PropertiesModel.spark_driver_host)
                .set("spark.driver.port", PropertiesModel.spark_driver_port) //
                .set("spark.blockManager.port", PropertiesModel.spark_blockManager_port) // Raw socket via ServerSocketChannel
                .set("spark.cores.max", "4")
                .set("spark.eventLog.enabled", PropertiesModel.spark_eventLog_enabled)
                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.shuffle.service.enabled", "false")
//                .set("spark.dynamicAllocation.enabled", "false")
//                .set("spark.io.compression.codec", "snappy")
//                .set("spark.rdd.compress", "true")
                //.set("spark.executor.cores", "4c")
                //.setJars(new String[]{PropertiesModel.jars, PropertiesModel.databaseJar})
                .setJars(PropertiesUtils.getJars(PropertiesModel.jars, PropertiesUtils.delimiter))
                //.set("spark.dynamicAllocation.enabled", "false")
                .setMaster(PropertiesModel.spark_master);
        //.setMaster("local");
        //context = new SparkContext(conf);

        System.out.println("----------------------sparkService-constr");
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

    public TaskUrlDto startTask(HttpServletRequest request, String task) {

        int maxTasks = SparkConfiguartion.MAX_RUNNING_TASKS;
        int running = (int) runningTasks.entrySet().stream().filter(s -> s.getValue().getRunning().booleanValue() == true).count();
        if (running >= maxTasks) {
            throw new TaskExistException("Cannot start new task, running tasks limit=" + maxTasks);
        }

        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());
        UUID uuid = UUID.randomUUID();
        JavaSparkContext jsc = configureSpark(conf);
        runningTasks.put(uuid, new TaskModel(true, uuid, time, jsc, task));
        new Thread(() -> sparkApplicationService.startTask(uuid, runningTasks)).start();
        TaskUrlDto taskUrlDto = new TaskUrlDto();
        taskUrlDto.setTaskUrl(getUrl(request) +"/get/"+ uuid.toString());
        taskUrlDto.setWebUiUrl(jsc.sc().uiWebUrl().get());
        taskUrlDto.setAppId(jsc.sc().applicationId());
        taskUrlDto.setAppName(jsc.sc().appName());
        return taskUrlDto;
        // return new TaskUrlDto(getUrl(request) + uuid.toString(), jsc.sc().uiWebUrl().get());
    }

    public TaskModel getTask(String id) {
        if (!Optional.ofNullable(runningTasks.get(UUID.fromString(id))).isPresent()) {
            throw new TaskNotFoundException("Task with id=" + id + " not found.");
        }
        return runningTasks.get(UUID.fromString(id));
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
