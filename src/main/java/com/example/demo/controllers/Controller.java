package com.example.demo.controllers;

import com.example.demo.dto.TaskUrlDto;
import com.example.demo.dto.TasksInfoDto;
import com.example.demo.models.MyModel;
import com.example.demo.services.SparkApplicationService;
import com.example.demo.services.SparkService;
import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * Created by as on 28.06.2018.
 */
@RestController
public class Controller {
    private SparkApplicationService sparkApplicationService;
    private SparkService sparkService;
    private volatile static Hashtable<UUID, MyModel> runningTasks = new Hashtable<>();
    private SparkConf conf;

    @Autowired
    public Controller(SparkService sparkService, SparkApplicationService sparkApplicationService) {
        this.sparkService = sparkService;
        this.sparkApplicationService = sparkApplicationService;
        conf = new SparkConf()
                .setAppName("Spark_Experiment_Pi")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local");
    }

    // GET ALL TASKS
    @RequestMapping("/all")
    public ResponseEntity<Iterable<MyModel>> getAllTasks() {
        return new ResponseEntity<>(sparkService.getAllTasks(runningTasks), HttpStatus.OK);
    }

    // GET TASKS SUMMARY INFO
    @RequestMapping("/info")
    public ResponseEntity<TasksInfoDto> getTasksInfo() {
        return new ResponseEntity<>(sparkService.getTasksInfo(runningTasks), HttpStatus.OK);
    }

    // SET MASTER
    @RequestMapping(value = "/master/{master}", method = RequestMethod.GET)
    public ResponseEntity<String> setMaster(@PathVariable String master) {
        return new ResponseEntity<>(sparkService.setMaster(conf, master), HttpStatus.OK);
    }

    // GET MASTER
    @RequestMapping(value = "/master", method = RequestMethod.GET)
    public ResponseEntity<String> getMaster() {
        return new ResponseEntity<>(sparkService.getMaster(conf), HttpStatus.OK);
    }

    // START TASK
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public ResponseEntity<TaskUrlDto> startTask(HttpServletRequest request) {
        return new ResponseEntity<>(sparkService.startTask(runningTasks, conf, request), HttpStatus.OK);
    }

    // GET TASK BY ID
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<MyModel> getTask(@PathVariable String id) {
        return new ResponseEntity<>(sparkService.getTask(runningTasks, id), HttpStatus.OK);
    }

    // STOP TASK BY ID
    @RequestMapping(value = "/stop/{id}", method = RequestMethod.GET)
    public ResponseEntity<MyModel> stopTaskById(@PathVariable String id) {
        return new ResponseEntity<>(sparkService.stopTaskById(runningTasks, id), HttpStatus.OK);
    }

    // STOP ALL
    @RequestMapping(value = "/stop", method = RequestMethod.GET)
    public ResponseEntity<String> stopAllTasks() {
        return new ResponseEntity<>(sparkService.stopAllTasks(runningTasks), HttpStatus.OK);
    }

    // CLEAN INFO ABOUT TASKS
    @RequestMapping(value = "/clean", method = RequestMethod.GET)
    public ResponseEntity<String> clean() {
        return new ResponseEntity<>(sparkService.clean(runningTasks), HttpStatus.OK);
    }
}
