import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class FileWithNumbersGenerator {

    private final static int NUMBERS_PER_FILE = 1000;
    private final static int NUMBER_OF_FILES = 20;

    public static void main(String[] args) throws IOException {
        for (int fileNumber = 1; fileNumber <= NUMBER_OF_FILES; fileNumber++) {
            PrintWriter writer = new PrintWriter(new FileWriter("random_numbers_" + fileNumber + ".txt"));
            Random generator = new Random();
            generateRandomNumbersToTheFile(writer, generator);
            writer.close();
        }
    }

    private static void generateRandomNumbersToTheFile(PrintWriter writer, Random generator) {
        for (int i = 0; i < NUMBERS_PER_FILE; i++) {
            writer.write(String.valueOf(1 + generator.nextInt(1000)));
            if (i < NUMBERS_PER_FILE - 1) {
                writer.write(" ");
            }
        }
    }
}
