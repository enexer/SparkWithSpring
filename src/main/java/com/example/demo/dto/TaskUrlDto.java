package com.example.demo.dto;

/**
 * Created by as on 07.07.2018.
 */
public class TaskUrlDto {

    private String taskUrl;
    private String webUiUrl;
    private String appId;
    private String appName;

    public String getTaskUrl() {
        return taskUrl;
    }

    public void setTaskUrl(String taskUrl) {
        this.taskUrl = taskUrl;
    }

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
