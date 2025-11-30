package dev.pig.obrc;

import dev.pig.obrc.pipeline.Calculate_baseline;
import dev.pig.obrc.pipeline.Generate;
import dev.pig.obrc.pipeline.Validate;

import java.nio.file.Files;
import java.nio.file.Path;

public class Benchmark {

    private static final String INPUT = "./measurements.txt";
    private static final String OUTPUT = "./results.out";
    private static final String EXPECTED = "./results_baseline.out";

    private static final int ROWS = 1_000_000_000;

    public static void main(final String[] args) throws Exception {
        // Generate input
        Generate.createMeasurementsIfNotExists(ROWS, INPUT);

        // Run Benchmark against baseline:
        if (!Files.exists(Path.of(EXPECTED))) {
            System.out.println("Expected file not found, running baseline...");
            final long baseline = dev.pig.obrc.pipeline.Benchmark.run(Calculate_baseline::run, INPUT, EXPECTED);
            System.out.printf("Baseline benchmark took %,dms%n", baseline);
        }

        // Run Benchmark against current:
        System.out.println("Starting benchmark...");
        final long elapsed = dev.pig.obrc.pipeline.Benchmark.run(CalculateAverage::run, INPUT, OUTPUT);
        System.out.printf("Benchmark took %,dms%n", elapsed);

        // Validate the output is correct
        Validate.compare(OUTPUT, EXPECTED);
    }
}
