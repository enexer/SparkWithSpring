package com.example.demo.models;

import com.example.demo.dto.enums.TaskName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Created by as on 29.06.2018.
 */
public class TaskModel {
    private Boolean running;
    private String content;
    private UUID uuid;
    private LocalDateTime startTime;
    private LocalDateTime finishTime;
    private Long elapsedTime;
    private JavaSparkContext jsc;
    private TaskName task;
    private String file;
    private String fileDelimiter;
    private String appHistoryUrl;

    public TaskModel(Boolean running, UUID uuid, LocalDateTime startTime, JavaSparkContext jsc, TaskName task, String file, String fileDelimiter) {
        this.running = running;
        this.uuid = uuid;
        this.startTime = startTime;
        this.jsc = jsc;
        this.content = "Task initialized.";
        this.task=task;
        this.file=file;
        this.fileDelimiter = fileDelimiter;
    }

    public void stopTask(){
        this.jsc.stop();
    }

    @JsonIgnore
    public JavaSparkContext getContext() {
        return jsc;
    }

    public Boolean getRunning() {
        return running;
    }

    public TaskModel setRunning(Boolean running) {
        this.running = running;
        return this;
    }

    public String getContent() {
        return content;
    }

    public TaskModel setContent(String content) {
        this.content = content;
        return this;
    }
    public TaskModel addContent(String content) {
        this.content = this.content+" "+content;
        return this;
    }

    public UUID getUuid() {
        return uuid;
    }

    public TaskModel setUuid(UUID uuid) {
        this.uuid = uuid;
        return this;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public TaskModel setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    public LocalDateTime getFinishTime() {
        return finishTime;
    }

    public TaskModel setFinishTime(LocalDateTime finishTime) {
        this.finishTime = finishTime;
        return this;
    }

    public TaskModel setElapsedTime(Long elapsedTime) {
        this.elapsedTime = elapsedTime;
        return this;
    }

    public Long getElapsedTime() {
        return elapsedTime;
    }

    public TaskName getTask() {
        return task;
    }

    public void setTask(TaskName task) {
        this.task = task;
    }

    public String getAppHistoryUrl() {
        return appHistoryUrl;
    }

    public TaskModel setAppHistoryUrl(String appHistoryUrl) {
        this.appHistoryUrl = appHistoryUrl;
        return this;
    }

    public String getFile() {
        return file;
    }

    public TaskModel setFile(String file) {
        this.file = file;
        return this;
    }

    public String getFileDelimiter() {
        return fileDelimiter;
    }

    public TaskModel setFileDelimiter(String fileDelimiter) {
        this.fileDelimiter = fileDelimiter;
        return this;
    }

}
