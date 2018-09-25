package com.example.demo.services;

import com.example.demo.configuration.PropertiesModel;
import com.example.demo.dto.enums.TaskName;
import com.example.demo.exceptions.SparkContextStoppedException;
import com.example.demo.models.TaskModel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import sparktemplate.association.AssociationSettings;
import sparktemplate.classifiers.ClassifierSettings;
import sparktemplate.clustering.ClusteringSettings;
import sparktemplate.datasets.DBDataSet;
import sparktemplate.datasets.MemDataSet;
import sparktemplate.deploying.Deploying;

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
    public void startTask(UUID uuid, Hashtable<UUID, TaskModel> runningTasks, boolean stopContext) {

        long startMillis = 0;

        if (runningTasks.get(uuid).getRunning()) {

            JavaSparkContext jsc = runningTasks.get(uuid).getContext();

            if (jsc.sc().isStopped()) {
                throw new SparkContextStoppedException("Cannot call methods on a stopped SparkContext.");
            }

            String res;
            runningTasks.get(uuid).addContent("In progress...\n");

            try {
                startMillis = System.currentTimeMillis();
                res = sparkTask(jsc, runningTasks.get(uuid), stopContext);
            } catch (Throwable t) {
                runningTasks.get(uuid)
                        .setRunning(false)
                        .addContent(t.getMessage());
                //throw new TaskException(t.getMessage());
                return;
            }

            runningTasks.get(uuid).addContent(res);
        }

        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());
        Long elapsedTime = System.currentTimeMillis() - startMillis;
        String appId = runningTasks.get(uuid).getContext().sc().applicationId();

        runningTasks.get(uuid)
                .setRunning(false)
                .addContent("\n...finished!")
                .setAppHistoryUrl("master:18080/" + appId)
                .setFinishTime(time)
                .setElapsedTime(elapsedTime);
    }


    public String sparkTask(JavaSparkContext jsc, TaskModel taskModel, boolean stopContext) {
        String ww = null;

        TaskName task = taskModel.getTask();

        if (task.equals(TaskName.TEST_COMPUTE_PI)) {
            ww = computePi(jsc);
        } else {
            String path = taskModel.getFile();
            //String path = "data_test/kdd_train.csv";
            MemDataSet memDataSet = new MemDataSet().setDs(loadData(jsc, path, taskModel.getFileDelimiter()));


            // Compute optimal partitions.
//            int executorInstances = Integer.valueOf(jsc.getConf().get("spark.executor.instances"));
//            int executorCores = Integer.valueOf(jsc.getConf().get("spark.executor.cores"));
//            int optimalPartitions = executorInstances * executorCores * 4;
//            memDataSet.getDs().repartition(optimalPartitions);

            // Settings.
            ClassifierSettings classifierSettings = new ClassifierSettings();
            AssociationSettings associationSettings = new AssociationSettings();
            ClusteringSettings clusteringSettings = new ClusteringSettings();

            // Split for classification.
            Dataset<Row>[] split = memDataSet.getDs().randomSplit(new double[]{0.9,0.1});
            MemDataSet trainData = new MemDataSet().setDs(split[0]);
            MemDataSet testData = new MemDataSet().setDs(split[1]);

            switch (task) {
                case CLASSIFICATION_DECISION_TREE:
                    classifierSettings.setDecisionTree().setMaxDepth(2);
                    ww = Deploying.classification(jsc.sc(), trainData, testData, classifierSettings);
                    break;
                case CLASSIFICATION_LINEAR_SVM:
                    classifierSettings.setLinearSVC().setMaxIter(10);
                    ww = Deploying.classification(jsc.sc(), trainData, testData, classifierSettings);
                    break;
                case CLASSIFICATION_LOGISTIC_REGRESSION:
                    classifierSettings.setLogisticRegression().setMaxIter(10);
                    ww = Deploying.classification(jsc.sc(), trainData, testData, classifierSettings);
                    break;
                case CLASSIFICATION_NAIVE_BAYES:
                    classifierSettings.setNaiveBayes();
                    ww = Deploying.classification(jsc.sc(), trainData, testData, classifierSettings);
                    break;
                case CLASSIFICATION_RANDOM_FOREST:
                    classifierSettings.setRandomForest().setNumTrees(2);
                    ww = Deploying.classification(jsc.sc(), trainData, testData, classifierSettings);
                    break;
                case ASSOCIATIONS_FP_GROWTH:
                    associationSettings.setFPGrowth();
                    ww = Deploying.assocRules(jsc.sc(), memDataSet, associationSettings);
                    break;
                case CLUSTERING_K_MEANS_SPARK_ML:
                    clusteringSettings.setKMeans();
                    ww = Deploying.clustering(jsc.sc(), memDataSet, clusteringSettings);
                    break;
                case CLUSTERING_K_MEANS_IMPLEMENTATION:
                    clusteringSettings.setKMeansImpl();
                    ww = Deploying.clusteringImpl(jsc.sc(), memDataSet, clusteringSettings);
                    break;
                default:
                    System.out.println("Wrong type!");
                    break;
            }
        }

        if (stopContext) {
            jsc.stop();
        }
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

    public Dataset<Row> loadData(JavaSparkContext jsc, String path, String delimiter) {
        // Only csv.
        SparkSession sparkSession = new SparkSession(jsc.sc());
        MemDataSet memDataSet = new MemDataSet(sparkSession);
        memDataSet.loadDataSetCSV(path, delimiter);
        return memDataSet.getDs();
    }

    // Spark test job.
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

        //jsc.stop();  /// Stop context.
        return res;
    }
}