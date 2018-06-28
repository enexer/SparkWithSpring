package com.example.demo;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

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
    public int ok(UUID uuid, Hashtable<UUID, MyModel> runningTasks){
        int i=0;
        while(i<100 && runningTasks.get(uuid).getRunning()){
            runningTasks.get(uuid).setContent("iteracja: "+i+"\n");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }
        runningTasks.get(uuid).setRunning(false).setContent("finished");
        return i;
    }

    @Async
    public void ok2(UUID uuid, Hashtable<UUID, MyModel> runningTasks){

        if (runningTasks.get(uuid).getRunning()){
            runningTasks.get(uuid).setContent("in progress...");

            JavaSparkContext jsc = runningTasks.get(uuid).getContext();

            String res = computePi(jsc);


            runningTasks.get(uuid).setContent(res);
        }

        LocalDateTime time = LocalDateTime.from(LocalDateTime.now());

        runningTasks.get(uuid)
                .setRunning(false)
                .setContent("finished")
                .setFinishTime(time);
    }


    public String computePi(JavaSparkContext jsc){

        if(jsc.sc().isStopped()){
            return "Cannot call methods on a stopped SparkContext.";
        }

        int NUM_SAMPLES =100;
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(i);
        }

        long count = jsc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1.0;
        }).count();
        String res = ("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
        return res;
        //jsc.stop();
    }

}