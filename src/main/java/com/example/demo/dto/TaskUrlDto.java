package com.example.demo.dto;

/**
 * Created by as on 07.07.2018.
 */
public class TaskUrlDto {

    private String taskUrl;
    private String webUiUrl;

    public TaskUrlDto(String taskUrl, String webUiUrl) {
        this.taskUrl = taskUrl;
        this.webUiUrl = webUiUrl;
    }

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
}
