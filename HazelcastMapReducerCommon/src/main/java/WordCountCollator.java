import com.hazelcast.mapreduce.Collator;

import java.util.Map;


public class WordCountCollator
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

