import com.hazelcast.mapreduce.Collator;

import java.util.Map;


public class AverageCollator
        implements Collator<Map.Entry<Integer, NumberReductionResult>, Double> {

    @Override
    public Double collate(Iterable<Map.Entry<Integer, NumberReductionResult>> values) {
        double sumOfAllNumbersAfterOp = 0;
        double countOfAllNumbers = 0;

        for (Map.Entry<Integer, NumberReductionResult> entry : values) {
            Integer count = entry.getValue().getCount();
            Integer opResultOnNumber = entry.getValue().getOpResult();
            sumOfAllNumbersAfterOp += opResultOnNumber * count;
            countOfAllNumbers += count;
        }

        return sumOfAllNumbersAfterOp / countOfAllNumbers;
    }
}

