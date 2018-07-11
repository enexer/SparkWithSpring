package com.example.demo.configuration;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by as on 11.07.2018.
 */
public class Properties {
    
    public static boolean readProperties(String fullPath) {
        System.out.println("READ PROPERTIES");
        InputStream in;
        try {
            in = new FileInputStream(fullPath);
        } catch (FileNotFoundException e) {
            //e.printStackTrace();
            return false;
        }

        java.util.Properties properties = new java.util.Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        properties.stringPropertyNames().forEach(s -> System.out.println(s + " = " + properties.getProperty(s)));
        return true;
    }

    private static Map<String, String> loadProperties(Class c) throws IllegalAccessException {
        System.out.println("LOAD PROPERTIES");
        Map<String, String> propMap = new HashMap<>();
        for (Field f : c.getDeclaredFields()) {
            String name = f.getName();
            String value;
            try {
                value = f.get(new Object()).toString();
            } catch (NullPointerException e) {
                value = "null";
            }
            propMap.put(name, value);
        }
        return propMap;
    }

    public static String createProperties(String fullPath, Class c) {
        System.out.println("CREATE PROPERTIES");
        java.util.Properties prop = new java.util.Properties();
        OutputStream output = null;

        try {

            output = new FileOutputStream(fullPath);

            // set the properties value
            Map<String, String> propMap = null;
            try {
                propMap = loadProperties(c);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            for (Map.Entry<String, String> item : propMap.entrySet()) {
                String key = item.getKey();
                String value = item.getValue();
                prop.setProperty(key, value);
            }
            // save properties to project root folder
            prop.store(output, null);

        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        return fullPath;
    }
}
