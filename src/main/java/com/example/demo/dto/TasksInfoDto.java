package com.example.demo.dto;

/**
 * Created by as on 07.07.2018.
 */
public class TasksInfoDto {
    private int total;
    private int running;
    private int finished;
    private int runningSparkContexts;

    public TasksInfoDto(int total, int running, int finished, int runningSparkContexts) {
        this.total = total;
        this.running = running;
        this.finished = finished;
        this.runningSparkContexts = runningSparkContexts;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getRunning() {
        return running;
    }

    public void setRunning(int running) {
        this.running = running;
    }

    public int getFinished() {
        return finished;
    }

    public void setFinished(int finished) {
        this.finished = finished;
    }

    public int getRunningSparkContexts() {
        return runningSparkContexts;
    }

    public void setRunningSparkContexts(int runningSparkContexts) {
        this.runningSparkContexts = runningSparkContexts;
    }
}
