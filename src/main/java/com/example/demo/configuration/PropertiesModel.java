package com.example.demo.configuration;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by as on 11.07.2018.
 */
public class PropertiesModel {
    public static String jars = "";
    public static String spark_master = "";
    public static String spark_driver_host = "";
    public static String spark_driver_port = "";
    public static String spark_blockManager_port = "";
    public static String spark_eventLog_enabled = "";
    public static String db_url = "";
    public static String db_user = "";
    public static String db_password = "";
    public static String db_table = "";

    public static String printAll() throws IllegalAccessException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Field f: PropertiesModel.class.getDeclaredFields()){
            String fieldName = f.getName();
            String fieldValue;
            try {
                fieldValue = f.get(new Object()).toString();
            } catch (NullPointerException e) {
                fieldValue = "null";
            }
            stringBuffer.append(fieldName+" = "+fieldValue+System.lineSeparator());
        }
        return stringBuffer.toString();
//        return PropertiesModel.master + "\n" +
//                PropertiesModel.jars + "\n" +
//                PropertiesModel.driver;
    }

//    public static String getMaster() {
//        return master;
//    }
//
//    public static void setMaster(String master) {
//        PropertiesModel.master = master;
//    }
//
//    public static String getJars() {
//        return jars;
//    }
//
//    public static void setJars(String jars) {
//        PropertiesModel.jars = jars;
//    }
//
//    public static String getDriver() {
//        return driver;
//    }
//
//    public static void setDriver(String driver) {
//        PropertiesModel.driver = driver;
//    }

//    public static void setValue(String name, String value, Class c) throws InvocationTargetException, IllegalAccessException {
//        System.out.println("ssssssssssssssssssssss" + name + ", " + value);
//        for (Method method : c.getClass().getMethods()) {
//            if ((method.getName().startsWith("set")) && (method.getName().length() == (name.length() + 3))) {
//                String expectedName = "set" + name;
//                if (expectedName.toLowerCase().equals(method.getName().toLowerCase())) {
//                    method.invoke(value);
//                }
//            }
//        }
//    }
}
