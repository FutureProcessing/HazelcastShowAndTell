import com.google.common.base.Stopwatch;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

public class HazelcastMapReducerJob {

    private final static int NUMBER_OF_FILES = 20;

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

        // Retrieving the JobTracker by name
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");

        // Creating the KeyValueSource for a Hazelcast IMap
        IMap<String, String> map = hazelcastInstance.getMap("numbers");
        KeyValueSource<String, String> source = KeyValueSource.fromMap(map);

        // Creating a new Job
        Job<String, String> job = jobTracker.newJob(source);

        ICompletableFuture<Double> future = job // returned future
                .mapper(new TokenizerMapper())             // adding a mapper
                .combiner(new WordCountCombinerFactory())  // adding a combiner through the factory
                .reducer(new WordCountReducerFactory())    // adding a reducer through the factory
                .submit(new WordCountCollator());          // submit the task and supply a collator

        future.andThen(buildCallback());

        // Wait and retrieve the result
        return future.get();
    }

    public static class TokenizerMapper
            implements Mapper<String, String, Integer, Integer> {

        private static final Integer ONE = Integer.valueOf(1);

        @Override
        public void map(String key, String numbersString, Context<Integer, Integer> context) {
            // Just splitting the text by whitespaces
            StringTokenizer tokenizer = new StringTokenizer(numbersString);

            // For every token in the text (=> per word)
            while (tokenizer.hasMoreTokens()) {
                // Emit a new value in the mapped results
                Integer number = Integer.valueOf(tokenizer.nextToken());
                context.emit(number, ONE);
            }
        }
    }

    public static class WordCountCombinerFactory
            implements CombinerFactory<Integer, Integer, Integer> {

        @Override
        public Combiner<Integer, Integer, Integer> newCombiner(Integer key) {
            return new WordCountCombiner();
        }

        private class WordCountCombiner
                extends Combiner<Integer, Integer, Integer> {

            private int sum = 0;

            @Override
            public void combine(Integer key, Integer value) {
                sum++;
            }

            @Override
            public Integer finalizeChunk() {
                int chunk = sum;
                sum = 0;
                return chunk;
            }
        }
    }

    public static class OpResult {
        private int count;
        private int opResult;

        public OpResult(int count, int opResult) {
            this.count = count;
            this.opResult = opResult;
        }

        public int getCount() {
            return count;
        }

        public int getOpResult() {
            return opResult;
        }
    }

    public static class WordCountReducerFactory
            implements ReducerFactory<Integer, Integer, OpResult> {

        @Override
        public Reducer<Integer, Integer, OpResult> newReducer(Integer key) {
            return new WordCountReducer(key);
        }

        private class WordCountReducer
                extends Reducer<Integer, Integer, OpResult> {

            private volatile int sum = 0;
            private Integer key;

            public WordCountReducer(Integer key) {
                this.key = key;
            }

            @Override
            public void reduce(Integer value) {
                sum += value.longValue();
            }

            @Override
            public OpResult finalizeReduce() {
                return new OpResult(sum, someOpOn(key));
            }
        }

        private int someOpOn(Integer number) {
            if (number > 500) {
                return (int) Math.sqrt(number);
            } else if (number > 250) {
                return (int) Math.atan(number);
            }
            return (int) Math.log(number);
        }
    }

    public static class WordCountCollator
            implements Collator<Map.Entry<Integer, OpResult>, Double> {

        @Override
        public Double collate(Iterable<Map.Entry<Integer, OpResult>> values) {
            double sum = 0;
            double sum2 = 0;

            for (Map.Entry<Integer, OpResult> entry : values) {
                Integer count = entry.getValue().getCount();
                Integer opResultOnNumber = entry.getValue().getOpResult();
                sum += opResultOnNumber * count;
                sum2 += count;
            }

            return sum / sum2;
        }
    }

    private static ExecutionCallback<Double> buildCallback() {
        return new ExecutionCallback<Double>() {
            @Override
            public void onResponse(Double finished) {
                System.out.println("Calculation finished! :)");
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
