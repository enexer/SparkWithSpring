package com.example.demo.services;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.exceptions.SparkContextStoppedException;
import com.example.demo.models.TaskModel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;
import sparktemplate.test.TestClustering;
import sparktemplate.test.TestDBDataSet;
import sun.plugin2.main.server.JVMInstance;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

/**
 * Created by as on 29.06.2018.
 */
@Service
public class SparkApplicationService {

    @Async
    public void startTask(UUID uuid, Hashtable<UUID, TaskModel> runningTasks) {

        long startMillis = 0;

        if (runningTasks.get(uuid).getRunning()) {

            JavaSparkContext jsc = runningTasks.get(uuid).getContext();
            String task = runningTasks.get(uuid).getTask();

            if (jsc.sc().isStopped()) {
                throw new SparkContextStoppedException("Cannot call methods on a stopped SparkContext.");
            }
            String res;
            runningTasks.get(uuid).addContent("in progress...");
            jsc.sc().log().info("###################################" + jsc.sc().logName());

            try {
                startMillis = System.currentTimeMillis();
                res = sparkTask(jsc, task);
            } catch (Throwable t) {
                runningTasks.get(uuid)
                        .setRunning(false)
                        .addContent(t.getMessage());
                //throw new TaskException(t.getMessage());
                return;
            }

            //ConsoleProgressBar consoleProgressBar = new ConsoleProgressBar(jsc.sc());
            //consoleProgressBar.log().info("HEHEHEE");

            runningTasks.get(uuid).addContent(res);
        }

        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());
        Long elapsedTime = System.currentTimeMillis() - startMillis;
        String appId = runningTasks.get(uuid).getContext().sc().applicationId();

        runningTasks.get(uuid)
                .setRunning(false)
                .addContent("finished")
                .setAppHistoryUrl("master:18080/" + appId)
                .setFinishTime(time)
                .setElapsedTime(elapsedTime);
    }


    public String sparkTask(JavaSparkContext jsc, String task) {
        String ww = null;
        if (task.equals("1")) {
            ww = computePi(jsc);
        } else if (task.equals("2")) {
            ww = TestDBDataSet.dbTest(jsc);
        } else if (task.equals("3")) {
            ww = TestClustering.dbTest(jsc);
        }
        jsc.stop();
        return ww;
    }

    public Dataset<Row> loadDataDb(JavaSparkContext jsc, String table) {
        SparkSession sparkSession = new SparkSession(jsc.sc());
        DBDataSet dbDataSet = new DBDataSet(sparkSession,
                PropertiesModel.db_url,
                PropertiesModel.db_user,
                PropertiesModel.db_password,
                PropertiesModel.db_table);

        dbDataSet.connect();
        return dbDataSet.getDs();
    }

    public Dataset<Row> loadData(JavaSparkContext jsc, String path) {
        // only csv
        SparkSession sparkSession = new SparkSession(jsc.sc());
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSet(path);
        return memDataSet.getDs();
    }

    ////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////

    public static String computePi(JavaSparkContext jsc) {

        int NUM_SAMPLES = 100;
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // System.out.println(i);
        }

        if (jsc.sc().isStopped()) {
            return "Cannot call methods on a stopped SparkContext.";
        }

        long count = jsc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x * x + y * y < 1.0;
        }).count();
        String res = ("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

        //jsc.stop();  /// STOP CONTEXT !!!!!!!!!!!!!!!!!!!!!!!!!

        return res;
    }
}