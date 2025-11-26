package dev.pig.obrc.pipeline;

import dev.pig.obrc.CalculateAverage;

public class Runner {

    private static final String INPUT = "./measurements.txt";
    private static final String OUTPUT = "./results.out";
    private static final String EXPECTED = "./results_baseline.out";
    private static final String RESULTS = "./results.csv";

    private static final int ROWS = 1_000_000_00;

    public static void main(final String[] args) throws Exception {

        // Generate input
        Generate.createMeasurementsIfNotExists(ROWS, INPUT);

        // Run Benchmark against baseline:
        System.out.println("Starting baseline benchmark");
        final long baseline = Benchmark.run(Calculate_baseline::run, INPUT, EXPECTED);
        System.out.printf("Baseline benchmark took %,dms%n", baseline);

        // Run Benchmark against current:
        System.out.println("Starting benchmark");
        final long elapsed = Benchmark.run(CalculateAverage::run, INPUT, OUTPUT);
        System.out.printf("Benchmark took %,dms%n", elapsed);

        // Validate the output is correct
        Validate.compare(OUTPUT, EXPECTED);

        // Write results
        // - Calculate CSV row
        // - Append to CSV file
    }

}

