package com.example.demo.configuration;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by as on 11.07.2018.
 */
public class PropertiesModel {
    public static String master;
    public static String jars;
    public static String driver;

    public static String printAll() {
        return PropertiesModel.master + "\n" +
                PropertiesModel.jars + "\n" +
                PropertiesModel.driver;
    }

    public static void setMaster(String master) {
        PropertiesModel.master = master;
    }

    public static void setJars(String jars) {
        PropertiesModel.jars = jars;
    }

    public static void setDriver(String driver) { PropertiesModel.driver = driver; }

    public static void setValue(String name, String value, Class c) throws InvocationTargetException, IllegalAccessException {
        for (Method method : c.getClass().getMethods()) {
            if ((method.getName().startsWith("get")) && (method.getName().length() == (name.length() + 3))) {
                String expectedName = "get" + name;
                if (expectedName.toLowerCase().equals(method.getName().toLowerCase())) {
                    method.invoke(value);
                }
            }
        }
    }
}
