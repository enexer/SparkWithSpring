package com.example.demo.services;

import com.example.demo.models.MyModel;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.NotFoundException;
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

    @Autowired
    public SparkService(SparkApplicationService sparkApplicationService) {
        this.sparkApplicationService = sparkApplicationService;
    }

    public JavaSparkContext configureSpark(SparkConf conf) {
        SparkContext context = new SparkContext(conf);
        JavaSparkContext jsc = new JavaSparkContext(context);
        return jsc;
    }

    public List<MyModel> getAllTasks(Hashtable<UUID, MyModel> runningTasks) {
        List<MyModel> list = runningTasks.entrySet()
                .stream()
                .map(s -> s.getValue())
                .collect(Collectors.toList());

        return list;
    }

    public static String getUrl(HttpServletRequest req) {
        String reqUrl = req.getRequestURL().toString();
        return reqUrl;
    }

    public String getTasksInfo(Hashtable<UUID, MyModel> runningTasks) {

        int running = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == true)
                .count();

        int finished = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == false)
                .count();

        int total = running + finished;


        return "total tasks: " + total + "\n" +
                "running tasks: " + running + "\n" +
                "finished tasks: " + finished;
    }

    public String setMaster(SparkConf conf, String master) {
        conf.setMaster(master);
        return conf.get("spark.master");
    }

    public String getMaster(SparkConf conf) {
        return conf.get("spark.master");
    }

    public String startTask(Hashtable<UUID, MyModel> runningTasks, SparkConf conf, HttpServletRequest request) {

        int running = (int) runningTasks.entrySet().stream().filter(s -> s.getValue().getRunning().booleanValue() == true).count();
        if (running > 0) {
            return "CANNOT START NEW TASK, MAX=1";
        }

        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());
        UUID uuid = UUID.randomUUID();
        JavaSparkContext jsc = configureSpark(conf);
        runningTasks.put(uuid, new MyModel(true, uuid, time, jsc));
        //new Thread(() -> sparkApplicationService.ok(uuid, runningTasks)).start();
        new Thread(() -> sparkApplicationService.ok2(uuid, runningTasks)).start();
        return getUrl(request) + uuid.toString() + "\n" + jsc.sc().uiWebUrl().get();
    }

    public MyModel getTask(Hashtable<UUID, MyModel> runningTasks, String id) {
        if (!Optional.ofNullable(runningTasks.get(UUID.fromString(id))).isPresent()) {
            throw new NotFoundException("ID NOT FOUND");
        }
        return runningTasks.get(UUID.fromString(id));
    }

    public String stopTaskById(Hashtable<UUID, MyModel> runningTasks, String id) {
        runningTasks.get(UUID.fromString(id)).setRunning(false);
        runningTasks.get(UUID.fromString(id)).stopTask();
        return runningTasks.get(UUID.fromString(id)).getContent();
    }

    public String stopAllTasks(Hashtable<UUID, MyModel> runningTasks) {

        runningTasks.entrySet()
                .stream()
                .forEach(s -> {
                    runningTasks.get(s.getKey()).setRunning(false);
                    runningTasks.get(s.getKey()).stopTask();
                });

        return "stopped all tasks";
    }

    public String clean(Hashtable<UUID, MyModel> runningTasks) {

        int running = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == true)
                .count();

        if (running > 0) {
            return "CANNOT CLEAN, STOP TASKS BEFORE THIS ACTION";
        }

        runningTasks.clear();
        return "cleaned all tasks";
    }

}
