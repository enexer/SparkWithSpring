package spark_lanucher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * Created by as on 10.07.2018.
 */
public class SparkLaucherTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        SparkAppHandle handle = new SparkLauncher()
                .setVerbose(true)
                //.setAppName("XDD")
                //.setJavaHome("C:\\Program Files\\Java\\jdk1.8.0_131")
                .setSparkHome("C:\\spark-2.3.0-bin-hadoop2.7\\spark-2.3.0-bin-hadoop2.7")
                .setAppResource("spark_jars/SparkWithSpring.jar")    // "/my/app.jar"
                //.setAppResource("local:/root/.ivy2/jars/org.postgresql_postgresql-42.1.1.jar")
                .setConf("spark.driver.host", "10.2.28.34")
                .addJar("spark_jars/postgresql-42.1.1.jar")
                .setMainClass("sparktemplate.test.TestDBDataSet")        // "my.spark.app.Main"
                .setMaster("spark://10.2.28.17:7077")
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .startApplication();
//                .addAppArgs(appArgs);
        //new String[]{"out/artifacts/SparkProject_jar/SparkProject.jar", "local:/root/.ivy2/jars/org.postgresql_postgresql-42.1.1.jar"}
        // Launches a sub-process that will start the configured Spark application.

        while (!handle.getState().isFinal()){
            handle.getState();  // immediately returns UNKNOWN
            //Thread.sleep(1000); // wait a little bit...
            handle.getState();  // the state may have changed to CONNECTED or others
        }
    }
}
