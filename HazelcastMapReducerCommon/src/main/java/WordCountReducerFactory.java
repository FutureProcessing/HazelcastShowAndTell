import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;


public class WordCountReducerFactory
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