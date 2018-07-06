package com.example.demo;

import ch.qos.logback.core.OutputStreamAppender;
import org.apache.log4j.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ui.ConsoleProgressBar;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import scala.App;
import sun.rmi.runtime.Log;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

/**
 * Created by as on 29.06.2018.
 */
@Service
public class MyService {

    @Async
    public void ok2(UUID uuid, Hashtable<UUID, MyModel> runningTasks) {

        BasicConfigurator.configure();
        Logger log = Logger.getLogger("org.apache.spark.SparkContext");
        log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        StringWriter consoleWriter = new StringWriter();
        WriterAppender appender = new WriterAppender(new PatternLayout("%d{ISO8601} %p - %m%n"),consoleWriter);
        log.addAppender(appender);

        if (runningTasks.get(uuid).getRunning()) {
            runningTasks.get(uuid).addContent("in progress...");
            JavaSparkContext jsc = runningTasks.get(uuid).getContext();
            String res;


            jsc.sc().log().info("###################################"+jsc.sc().logName());

            try {
                res = computePi(jsc);
            } catch (Throwable t) {
                runningTasks.get(uuid)
                        .setRunning(false)
                        .addContent(t.getMessage());
                return;
            }

            ConsoleProgressBar consoleProgressBar = new ConsoleProgressBar(jsc.sc());
            consoleProgressBar.log().info("HEHEHEE");
            //log.info("kurwa");

            runningTasks.get(uuid).addContent(res);
        }
        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());

        runningTasks.get(uuid)
                .setRunning(false)
                .addContent("finished")
                .setFinishTime(time);

        System.out.println("******************"+consoleWriter.getBuffer().toString());
    }


    public String computePi(JavaSparkContext jsc) {

        int NUM_SAMPLES = 100;
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            System.out.println(i);
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

        jsc.stop();  /// STOP CONTEXT !!!!!!!!!!!!!!!!!!!!!!!!!

        return res;
    }

    public String Kmeans(JavaSparkContext jsc) {

        // Loads data.
        SparkSession sparkSession = new SparkSession(jsc.sc());
        Dataset<Row> dataset = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("src/data/creditcard.csv");

        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model = kmeans.fit(dataset);

        // Make predictions
        Dataset<Row> predictions = model.transform(dataset);

        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette with squared euclidean distance = " + silhouette);
        return "Silhouette with squared euclidean distance = " + silhouette;
    }

}