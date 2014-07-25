import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;


public class NumberCountAndOpReducerFactory
        implements ReducerFactory<Integer, Integer, NumberReductionResult> {

    @Override
    public Reducer<Integer, Integer, NumberReductionResult> newReducer(Integer key) {
        return new WordCountReducer(key);
    }

    private class WordCountReducer
            extends Reducer<Integer, Integer, NumberReductionResult> {
        private volatile int countOfTheSameKeyInTheChunk = 0;
        private Integer key;

        public WordCountReducer(Integer key) {
            this.key = key;
        }

        @Override
        public void reduce(Integer value) {
            countOfTheSameKeyInTheChunk += value.longValue();
        }

        @Override
        public NumberReductionResult finalizeReduce() {
            System.out.println("Finalizing reduction on key: " + key);
            return new NumberReductionResult(countOfTheSameKeyInTheChunk, someOpOn(key));
        }
    }

    private int someOpOn(Integer number) {
        if (number > 500) {
            return (int) Math.sqrt(number);
        }
        if (number > 250) {
            return (int) Math.atan(number);
        }
        return (int) Math.log(number);
    }
}