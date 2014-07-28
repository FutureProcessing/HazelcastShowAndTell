import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class NumberCountCombinerFactory
        implements CombinerFactory<Integer, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer, Integer> newCombiner(Integer key) {
        return new NumberCountCombiner();
    }

    private class NumberCountCombiner
            extends Combiner<Integer, Integer, Integer> {

        private int countOfTheSameKeyInTheChunk = 0;

        @Override
        public void combine(Integer key, Integer value) {
            countOfTheSameKeyInTheChunk++;
        }

        @Override
        public Integer finalizeChunk() {
            int chunk = countOfTheSameKeyInTheChunk;
            countOfTheSameKeyInTheChunk = 0;
            return chunk;
        }
    }
}