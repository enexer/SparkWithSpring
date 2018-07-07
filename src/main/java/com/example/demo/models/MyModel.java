package com.example.demo.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Created by as on 29.06.2018.
 */
public class MyModel {
    private Boolean running;
    private String content;
    private UUID uuid;
    private LocalDateTime startTime;
    private LocalDateTime finishTime;
    private JavaSparkContext jsc;

    public MyModel(Boolean running, UUID uuid, LocalDateTime startTime, JavaSparkContext jsc) {
        this.running = running;
        this.uuid = uuid;
        this.startTime = startTime;
        this.jsc = jsc;
        this.content = "start\n";
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

    public MyModel setRunning(Boolean running) {
        this.running = running;
        return this;
    }

    public String getContent() {
        return content;
    }

    public MyModel setContent(String content) {
        this.content = content;
        return this;
    }
    public MyModel addContent(String content) {
        this.content = this.content+" "+content;
        return this;
    }

    public UUID getUuid() {
        return uuid;
    }

    public MyModel setUuid(UUID uuid) {
        this.uuid = uuid;
        return this;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public MyModel setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    public LocalDateTime getFinishTime() {
        return finishTime;
    }

    public MyModel setFinishTime(LocalDateTime finishTime) {
        this.finishTime = finishTime;
        return this;
    }
}
