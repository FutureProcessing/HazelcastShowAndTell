import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.StringTokenizer;


public class TokenizerMapper
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
