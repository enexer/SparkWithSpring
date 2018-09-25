package com.example.demo.controllers;

import com.example.demo.dto.TaskUrlDto;
import com.example.demo.dto.TasksInfoDto;
import com.example.demo.dto.enums.TaskName;
import com.example.demo.models.TaskModel;
import com.example.demo.services.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by as on 28.06.2018.
 */
@CrossOrigin
@RestController
public class SparkController {
    private SparkService sparkService;

    @Autowired
    public SparkController(SparkService sparkService) {
        this.sparkService = sparkService;
    }

    // GET ALL TASKS
    @GetMapping("/all")
    public ResponseEntity<Iterable<TaskModel>> getAllTasks() {
        return new ResponseEntity<>(sparkService.getAllTasks(), HttpStatus.OK);
    }

    // GET TASKS SUMMARY INFO
    @GetMapping("/info")
    public ResponseEntity<TasksInfoDto> getTasksInfo() {
        return new ResponseEntity<>(sparkService.getTasksInfo(), HttpStatus.OK);
    }

    // GET PROPERTIES
    @GetMapping("/prop")
    public ResponseEntity<String> getProperties() {
        return new ResponseEntity<>(sparkService.getProperties(), HttpStatus.OK);
    }

    // SET MASTER
    @GetMapping("/master/set")
    public ResponseEntity<String> setMaster(@RequestParam(name = "master", required = false, defaultValue = "local") String master) {
        return new ResponseEntity<>(sparkService.setMaster(master), HttpStatus.OK);
    }

    // GET MASTER
    @GetMapping(value = "/master")
    public ResponseEntity<String> getMaster() {
        return new ResponseEntity<>(sparkService.getMaster(), HttpStatus.OK);
    }

    // START TASK
    @PostMapping(value = "/start")
    public ResponseEntity<TaskUrlDto> startTask(@RequestParam(value = "type") TaskName taskName,
                                                @RequestParam(value = "file") String file,
                                                @RequestParam(value = "delimiter", defaultValue = ",") String fileDelimiter,
                                                @RequestParam(value = "memory", defaultValue = "15g") String memory,
                                                @RequestParam(value = "cores", defaultValue = "12") String cores,
                                                @RequestParam(value = "stopContext", defaultValue = "true") Boolean stopContext,
                                                HttpServletRequest request) {
        return new ResponseEntity<>(sparkService.startTask(request, taskName, file, fileDelimiter, memory, cores, stopContext), HttpStatus.OK);
    }

    // GET TASK BY ID
    @GetMapping(value = "/get/{id}")
    public ResponseEntity<TaskModel> getTask(@PathVariable String id) {
        return new ResponseEntity<>(sparkService.getTask(id), HttpStatus.OK);
    }

    // GET TASK BY ID
    @GetMapping(value = "/getresult/{id}")
    public ResponseEntity<String> getTaskResult(@PathVariable String id) {
        return new ResponseEntity<>(sparkService.getTaskResult(id), HttpStatus.OK);
    }

    // STOP TASK BY ID
    @GetMapping(value = "/stop/{id}")
    public ResponseEntity<TaskModel> stopTaskById(@PathVariable String id) {
        return new ResponseEntity<>(sparkService.stopTaskById(id), HttpStatus.OK);
    }

    // STOP ALL
    @GetMapping(value = "/stop")
    public ResponseEntity<String> stopAllTasks() {
        return new ResponseEntity<>(sparkService.stopAllTasks(), HttpStatus.OK);
    }

    // CLEAN INFO ABOUT TASKS
    @GetMapping(value = "/clean")
    public ResponseEntity<String> clean() {
        return new ResponseEntity<>(sparkService.clean(), HttpStatus.OK);
    }
}
