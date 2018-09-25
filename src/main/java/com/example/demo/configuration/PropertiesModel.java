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
    }
}
