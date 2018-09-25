package com.example.demo.dto;

/**
 * Created by as on 07.07.2018.
 */
public class TaskUrlDto {

    private String taskUUID;
    private String taskResultUrl;
    private String taskDetailsUrl;
    private String webUiUrl;
    private String appId;
    private String appName;

    public String getTaskUUID() { return taskUUID; }

    public void setTaskUUID(String taskUUID) { this.taskUUID = taskUUID; }

    public String getTaskResultUrl() {
        return taskResultUrl;
    }

    public void setTaskResultUrl(String taskResultUrl) {
        this.taskResultUrl = taskResultUrl;
    }

    public String getTaskDetailsUrl() { return taskDetailsUrl; }

    public void setTaskDetailsUrl(String taskDetailsUrl) { this.taskDetailsUrl = taskDetailsUrl; }

    public String getWebUiUrl() {
        return webUiUrl;
    }

    public void setWebUiUrl(String webUiUrl) {
        this.webUiUrl = webUiUrl;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
