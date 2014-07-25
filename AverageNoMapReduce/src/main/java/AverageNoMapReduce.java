import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

public class AverageNoMapReduce {

    private final static int NUMBER_OF_FILES = 10;
    private static long timeElapsed;

    public static void main(String[] args) throws IOException {
        double allSum = 0;
        long count = 0;
        for (int fileNumber = 1; fileNumber <= NUMBER_OF_FILES; fileNumber++) {
            List<Integer> allNumbersFromFile = loadNumbersFromFile(fileNumber);

            Stopwatch timer = Stopwatch.createStarted();
            for (Integer number : allNumbersFromFile) {
                allSum += someOpOn(number);
            }
            count += allNumbersFromFile.size();
            timer.stop();
            timeElapsed += timer.elapsed(TimeUnit.MILLISECONDS);
        }

        System.out.println("All content averages up to " + allSum / count);
        System.out.println("Time elapsed " + timeElapsed);
    }

    private static int someOpOn(Integer number) {
        if (number > 500) {
            return (int) Math.sqrt(number);
        }
        if (number > 250) {
            return (int) Math.atan(number);
        }
        return (int) Math.log(number);
    }

    private static List<Integer> loadNumbersFromFile(int fileNumber) throws IOException {
        String numbersString = new String(Files.readAllBytes(Paths.get("../FileWithNumbersGenerator/random_numbers_" + fileNumber + ".txt")));
        Stopwatch timer = Stopwatch.createStarted();
        StringTokenizer tokenizer = new StringTokenizer(numbersString);

        List<Integer> allNumbers = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            Integer number = Integer.valueOf(tokenizer.nextToken());
            allNumbers.add(number);
        }
        timer.stop();
        timeElapsed += timer.elapsed(TimeUnit.MILLISECONDS);
        return allNumbers;
    }
}
