package dev.pig.obrc.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Benchmark {

    @FunctionalInterface
    interface Calculator{
        String run(final String input) throws IOException;
    }

    static long run(final Calculator calculator, final String input, final String output) throws IOException {
        final long start = System.currentTimeMillis();
        final String result = calculator.run(input);
        final long elapsed = System.currentTimeMillis() - start;

        System.out.printf("Writing results to output file %s%n", output);
        Files.writeString(Path.of(output), result);
        return elapsed;
    }

}
