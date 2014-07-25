import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.StringTokenizer;


public class TokenizerMapper
        implements Mapper<String, String, Integer, Integer> {

    private static final Integer ONE = Integer.valueOf(1);

    @Override
    public void map(String key, String numbersString, Context<Integer, Integer> context) {
        StringTokenizer tokenizer = new StringTokenizer(numbersString);

        while (tokenizer.hasMoreTokens()) {
            Integer number = Integer.valueOf(tokenizer.nextToken());
            context.emit(number, ONE);
        }
    }
}
