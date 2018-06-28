package com.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import org.spark_project.jetty.util.ConcurrentHashSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by as on 28.06.2018.
 */
@RestController
public class Controller {
    private MyService service;
    private volatile static Hashtable<UUID, MyModel> runningTasks = new Hashtable<>();
    //private volatile static ConcurrentHashSet<MyModel> runningTasks = new ConcurrentHashSet<>();
    //private JavaSparkContext jsc;

    @Autowired
    public Controller(MyService service) {
        this.service = service;

//        SparkConf conf = new SparkConf()
//                .setAppName("Spark_Experiment_Pi")
//                .set("spark.driver.allowMultipleContexts", "true")
//                .setMaster("local");
//
//        SparkContext context = new SparkContext(conf);
//        JavaSparkContext jsc = new JavaSparkContext(context);
//
//        this.jsc = jsc;
    }

    private JavaSparkContext configureSpark(){
        SparkConf conf = new SparkConf()
                .setAppName("Spark_Experiment_Pi")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");

        SparkContext context = new SparkContext(conf);
        JavaSparkContext jsc = new JavaSparkContext(context);
        return jsc;
    }

    @RequestMapping("/all")
    public List<MyModel> getAll() {

        int running = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == true)
                .count();

        int finished = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue() == false)
                .count();

        int total = running + finished;

        List<MyModel> list = runningTasks.entrySet()
                .stream()
                .map(s -> s.getValue())
                .collect(Collectors.toList());

        return list;

//        return "total tasks: "+total+"\n"+
//                "running tasks: "+running+"\n"+
//                "finished tasks: "+finished+"\n"+list.toString();
    }


    @RequestMapping("/")
    public String index() {

        int running = (int) runningTasks.entrySet().stream().filter(s -> s.getValue().getRunning().booleanValue() == true).count();
        if (running > 1) {
            return "CANNOT START NEW TASK, MAX=2";
        }

        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());
        UUID uuid = UUID.randomUUID();
        JavaSparkContext jsc = configureSpark();
        runningTasks.put(uuid, new MyModel(true, uuid, time, jsc));
        //new Thread(() -> service.ok(uuid, runningTasks)).start();
        new Thread(() -> service.ok2(uuid, runningTasks)).start();
        return uuid.toString();
    }

    // SHOW BY ID
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public String getTask(@PathVariable String id) {
        if (!Optional.ofNullable(runningTasks.get(UUID.fromString(id))).isPresent()) {
            return "BAD TASK ID";
        }
        return runningTasks.get(UUID.fromString(id)).getContent();
    }

    // STOP BY ID
    @RequestMapping(value = "/stop/{id}", method = RequestMethod.GET)
    public String stopTaskById(@PathVariable String id) {
        runningTasks.get(UUID.fromString(id)).setRunning(false);
        return runningTasks.get(UUID.fromString(id)).getContent();
    }

    // STOP ALL
    @RequestMapping(value = "/stop", method = RequestMethod.GET)
    public String stopAllTasks() {
        runningTasks.entrySet()
                .stream()
                .map(s -> s.getValue().setRunning(false));

        runningTasks.entrySet().forEach(s -> s.getValue().stopTask());
        return "stopped all tasks";
    }

    // CLEAN TASKS
    @RequestMapping(value = "/clean", method = RequestMethod.GET)
    public String clean() {

        int running = (int) runningTasks.entrySet()
                .stream()
                .filter(s -> s.getValue().getRunning().booleanValue()==true)
                .count();

        if(running>0){
            return "CANNOT CLEAN, STOP TASKS BEFORE THIS ACTION";
        }

        runningTasks.clear();
        return "cleaned all tasks";
    }
}
