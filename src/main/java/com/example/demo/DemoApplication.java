package com.example.demo;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.configuration.PropertiesUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.net.InetAddress;
import java.net.SocketPermission;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) throws IOException, URISyntaxException {

        // new SocketPermission("10.2.28.31:55555", "accept,connect,listen"); //resolve ???


//        try {
//            Process p = Runtime.getRuntime().exec("netsh advfirewall set global StatefulFTP disable");
//            p.waitFor();
//            BufferedReader reader = new BufferedReader(
//                    new InputStreamReader(p.getInputStream()));
//            String line = reader.readLine();
//            while (line != null) {
//                System.out.println(line);
//                line = reader.readLine();
//            }
//
//        } catch (IOException e1) {
//        } catch (InterruptedException e2) {
//        }
        //System.getProperties().stringPropertyNames().stream().forEach(s-> System.out.println(s+" = "+System.getProperties().getProperty(s)));

        configureFileNames();
        configureProperties();
        SpringApplication.run(DemoApplication.class, args);
    }

    public static void configureFileNames() {
        //PROPERTIES FILE WITH SPARK CONFIGURATION, ARTIFACT AND ADDITIONAL JARS
        PropertiesUtils.properties = "spark.properties";
        //ARTIFACT NAME
        PropertiesUtils.artifact = "SparkWithSpring.jar";
        //DELIMITER FOR JARS
        PropertiesUtils.delimiter = ",";
    }

    public static void setInitialProperties() {
        String pathToJDBC = "local:/root/.ivy2/jars/org.postgresql_postgresql-42.1.1.jar";
        PropertiesModel.spark_master = "spark://10.2.28.17:7077";
        PropertiesModel.jars = new File(PropertiesUtils.artifact).getAbsolutePath() + PropertiesUtils.delimiter + pathToJDBC;
        PropertiesModel.spark_driver_host = "10.2.28.31";
        PropertiesModel.spark_driver_port = "55550";
        PropertiesModel.spark_blockManager_port ="55551";
    }

    public static void setInitialPropertiesLocal() {
        PropertiesModel.spark_master = "local";
        PropertiesModel.jars = new File(PropertiesUtils.artifact).getAbsolutePath();
        try {
            PropertiesModel.spark_driver_host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        PropertiesModel.spark_driver_port = "55550";
        PropertiesModel.spark_blockManager_port ="55551";
    }

    public static void configureProperties() {
        String jarPath = new File(PropertiesUtils.artifact).getAbsolutePath();//"artifact" + ".jar";
        System.out.println("ARTIFACT PATH: " + jarPath);
        String propertiesPath = new File(PropertiesUtils.properties).getAbsolutePath();
        System.out.println("PROPERTIES PATH: " + propertiesPath);

        try {
            new FileInputStream(jarPath);
        } catch (FileNotFoundException e) {
            System.out.println("ARTIFACT DOES NOT EXIST: " + jarPath);
            System.out.println("THIS FILE IS REQUIRED");
            System.exit(1);
        }

        if (!PropertiesUtils.readProperties(propertiesPath, PropertiesModel.class)) {
            // SET INITIAL PROPERTIES VALUES
            //setInitialProperties();
            setInitialPropertiesLocal();
            PropertiesUtils.createProperties(propertiesPath, PropertiesModel.class);
            PropertiesUtils.readProperties(propertiesPath, PropertiesModel.class);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(PropertiesUtils.printNoticeable("PROPERTIES_MODEL STATIC FIELDS"));
        try {
            System.out.println(PropertiesModel.printAll());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        System.out.println(PropertiesUtils.printNoticeable("PROPERTIES_MODEL STATIC FIELDS"));
        PropertiesUtils.getJars(PropertiesModel.jars, PropertiesUtils.delimiter);
    }
}
