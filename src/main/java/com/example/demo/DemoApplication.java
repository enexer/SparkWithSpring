package com.example.demo;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.configuration.PropertiesUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.io.*;
import java.net.InetAddress;
import java.net.SocketPermission;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;

@SpringBootApplication
public class DemoApplication {

    /**
     * To avoid problems with BlockManager connection (Raw socket via ServerSocketChannel) while deploying jar,
     * try to disable firewall or add firewall exception for java.
     */
    public static void main(String[] args) throws IOException, URISyntaxException {
        configureFileNames();
        configureProperties();
        SpringApplication.run(DemoApplication.class, args);
    }

    /**
     * Set dafault names.
     */
    public static void configureFileNames() {
        // Configuration file name.
        PropertiesUtils.properties = "spark.properties";
        // Artifact file name.
        PropertiesUtils.artifact = "SparkWithSpring.jar";
        // Delimiter for jars.
        PropertiesUtils.delimiter = ",";
    }

    /**
     * Cluster mode.
     */
    public static void setInitialProperties() {
        String pathToJDBC = "local:/root/.ivy2/jars/org.postgresql_postgresql-42.1.1.jar";
        PropertiesModel.spark_master = "spark://10.2.28.17:7077";
        PropertiesModel.jars = new File(PropertiesUtils.artifact).getAbsolutePath() + PropertiesUtils.delimiter + pathToJDBC;
        PropertiesModel.spark_driver_host = "10.2.28.34";
        PropertiesModel.spark_driver_port = "55550";
        PropertiesModel.spark_blockManager_port = "55551";
        PropertiesModel.spark_eventLog_enabled = "true";
        // Database connection settings.
        PropertiesModel.db_url = "jdbc:postgresql://10.2.28.17:5432/postgres";
        PropertiesModel.db_user = "postgres";
        PropertiesModel.db_password = "postgres";
        PropertiesModel.db_table = "dane2";
    }

    /**
     * Local mode.
     */
    public static void setInitialPropertiesLocal() {
        PropertiesModel.spark_master = "local";
        PropertiesModel.jars = new File(PropertiesUtils.artifact).getAbsolutePath();
        try {
            PropertiesModel.spark_driver_host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        PropertiesModel.spark_driver_port = "55550";
        PropertiesModel.spark_blockManager_port = "55551";
        PropertiesModel.spark_eventLog_enabled = "false"; // fail to delete appfiles
        // database connection settings
        PropertiesModel.db_url = "jdbc:postgresql://10.2.28.17:5432/postgres";
        PropertiesModel.db_user = "postgres";
        PropertiesModel.db_password = "postgres";
        PropertiesModel.db_table = "dane2";
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
            // Set initial properties values. Local or cluster.
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

    /**
     * Try to resolve problems with BlockManager while deploying jar.
     */
    public void enableSockets() {

        String host = PropertiesModel.spark_driver_host;
        String port = PropertiesModel.spark_blockManager_port;

        new SocketPermission(host + ":" + port, "accept,connect,listen");

        try {
            Process p = Runtime.getRuntime().exec("netsh advfirewall set global StatefulFTP disable");
            p.waitFor();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
            }

        } catch (IOException e1) {
        } catch (InterruptedException e2) {
        }
        System.getProperties().stringPropertyNames().stream().forEach(s -> System.out.println(s + " = " + System.getProperties().getProperty(s)));
    }
}
