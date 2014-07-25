import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class WordCountCombinerFactory
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