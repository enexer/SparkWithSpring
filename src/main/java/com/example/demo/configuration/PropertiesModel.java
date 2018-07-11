package com.example.demo.configuration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by as on 11.07.2018.
 */
public class PropertiesModel {
    public static String master = "local[*]";
    public static String mainAppJar = "C:\\Users\\as\\IdeaProjects\\SparkWithSpring";
    public static String otherJar = "C:\\Users\\as\\IdeaProjects\\SparkWithSpring";

    public static String printAll() {
        return PropertiesModel.master+"\n"+
                PropertiesModel.mainAppJar+"\n"+
                PropertiesModel.otherJar;
    }

    public static void setMaster(String master) {
        PropertiesModel.master = master;
    }

    public static void setMainAppJar(String mainAppJar) {
        PropertiesModel.mainAppJar = mainAppJar;
    }

    public static void setOtherJar(String otherJar) {
        PropertiesModel.otherJar = otherJar;
    }

    public static void setValue(String name, String value, Class c) throws InvocationTargetException, IllegalAccessException {
        for (Method method : c.getClass().getMethods()){
            if ((method.getName().startsWith("get")) && (method.getName().length() == (name.length() + 3))){
                String expectedName = "get"+name;
                if (expectedName.toLowerCase().equals(method.getName().toLowerCase())){
                    method.invoke(value);
                }
            }
        }
    }
}
