package com.example.demo;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.configuration.PropertiesUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.lang.annotation.Annotation;
import java.net.URISyntaxException;
import java.nio.file.Paths;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) throws IOException, URISyntaxException {
        configureProperties();
        SpringApplication.run(DemoApplication.class, args);
    }

    public static void setInitialProperties(){
        PropertiesModel.master = "spark://10.2.28.17:7077";
        PropertiesModel.mainAppJar = new File("SparkWithSpring.jar").getAbsolutePath();
        PropertiesModel.databaseJar = "local:/root/.ivy2/jars/org.postgresql_postgresql-42.1.1.jar";
        PropertiesModel.driver = "10.2.28.31";
    }

    public static void configureProperties(){

        // SET INITIAL PROPERTIES VALUES
        setInitialProperties();
        String jarPath = new File("SparkWithSpring.jar").getAbsolutePath();//"artifact" + ".jar";
        System.out.println("ARTIFACT PATH: " + jarPath);
        String propertiesPath = new File("config.properties").getAbsolutePath();
        System.out.println("PROPERTIES PATH: " + propertiesPath);

        try {
            new FileInputStream(jarPath);
        } catch (FileNotFoundException e) {
            System.out.println("ARTIFACT DOES NOT EXIST: "+jarPath);
            System.out.println("THIS FILE IS REQUIRED");
            return;
        }

        if (!PropertiesUtils.readProperties(propertiesPath, PropertiesModel.class)) {
            PropertiesUtils.createProperties(propertiesPath, PropertiesModel.class);
            PropertiesUtils.readProperties(propertiesPath, PropertiesModel.class);
        }
    }
}
