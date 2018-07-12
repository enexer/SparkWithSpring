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


        String jarPath = File.separator + getParentDirectoryFromJar() + File.separator + "artifact" + ".jar";
        System.out.println("ARTIFACT PATH: " + jarPath);


        try {
           new FileInputStream(jarPath);
        } catch (FileNotFoundException e) {
            System.out.println("ARTIFACT DOES NOT EXIST: "+jarPath);
            System.out.println("THIS FILE IS REQUIRED");
            return;
        }


        System.out.println("NEW: "+getParentDirectoryFromJar());

        String propName = "config.properties";
        String propertiesPath = System.getProperty("user.dir");
        System.out.println("Properties path: " + propertiesPath);
        String fullPath = File.separator + getParentDirectoryFromJar() + File.separator + propName;

        if (!PropertiesUtils.readProperties(fullPath, PropertiesModel.class)) {
            PropertiesUtils.createProperties(fullPath, PropertiesModel.class);
            PropertiesUtils.readProperties(fullPath, PropertiesModel.class);
        }

        SpringApplication.run(DemoApplication.class, args);
    }

    public static String getParentDirectoryFromJar() {
        String dirtyPath = DemoApplication.class.getResource("").toString();
        String jarPath = dirtyPath.replaceAll("^.*file:/", ""); //removes file:/ and everything before it
        jarPath = jarPath.replaceAll("jar!.*", "jar"); //removes everything after .jar, if .jar exists in dirtyPath
        jarPath = jarPath.replaceAll("%20", " "); //necessary if path has spaces within
        if (!jarPath.endsWith(".jar")) { // this is needed if you plan to run the app using Spring Tools Suit play button.
            jarPath = jarPath.replaceAll("/classes/.*", "/classes/");
        }
        String directoryPath = Paths.get(jarPath).getParent().toString(); //Paths - from java 8
        return directoryPath;
    }
}
