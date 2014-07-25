import com.google.common.base.Stopwatch;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class HazelcastMapReducerJob {

    private final static int NUMBER_OF_FILES = 10;

    public static void main(String[] args) throws Exception {
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        //cfg.getManagementCenterConfig().setEnabled(true).setUrl("http://localhost:8080");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);

        try {
            fillMapWithData(instance);

            Stopwatch timer = Stopwatch.createStarted();
            double average = mapReduceAverage(instance);
            timer.stop();
            System.out.println("All content averages up to " + average);
            System.out.println("Time elapsed " + timer.elapsed(TimeUnit.MILLISECONDS));
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static double mapReduceAverage(HazelcastInstance hazelcastInstance)
            throws Exception {
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");
        IMap<String, String> map = hazelcastInstance.getMap("numbers");
        KeyValueSource<String, String> source = KeyValueSource.fromMap(map);
        Job<String, String> job = jobTracker.newJob(source);

        ICompletableFuture<Double> future = job
                .mapper(new TokenizerMapper())
                .combiner(new NumberCountCombinerFactory())
                .reducer(new NumberCountAndOpReducerFactory())
                .submit(new AverageCollator());

        future.andThen(finishInfoCallback());

        return future.get();
    }

    private static ExecutionCallback<Double> finishInfoCallback() {
        return new ExecutionCallback<Double>() {
            @Override
            public void onResponse(Double finished) {
                System.out.println("Calculation finished!");
            }

            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
            }
        };
    }

    private static void fillMapWithData(HazelcastInstance instance) throws IOException {
        IMap<String, String> map = instance.getMap("numbers");

        for (int fileNumber = 1; fileNumber <= NUMBER_OF_FILES; fileNumber++) {
            String numbersString = new String(Files.readAllBytes(Paths.get("../FileWithNumbersGenerator/random_numbers_" + fileNumber + ".txt")));
            map.put(String.valueOf(fileNumber), numbersString);
        }
    }
}
