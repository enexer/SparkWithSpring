package com.example.demo;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.configuration.PropertiesUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) throws IOException {
        File jarPath = new File(DemoApplication.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        System.out.println(jarPath.getPath());

        String propName = "config.properties";
        String propertiesPath = System.getProperty("user.dir");
        System.out.println("Properties path: " + propertiesPath);
        String fullPath = propertiesPath + File.separator + propName;

        if (!PropertiesUtils.readProperties(fullPath, PropertiesModel.class)) {
            PropertiesUtils.createProperties(fullPath, PropertiesModel.class);
            PropertiesUtils.readProperties(fullPath, PropertiesModel.class);
        }

        SpringApplication.run(DemoApplication.class, args);
    }
}
