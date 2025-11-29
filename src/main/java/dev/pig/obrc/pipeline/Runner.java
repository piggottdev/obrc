package dev.pig.obrc.pipeline;

import dev.pig.obrc.CalculateAverage;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Runner {

    private static final String INPUT = "./measurements.txt";
    private static final String OUTPUT = "./results.out";
    private static final String EXPECTED = "./results_baseline.out";
    private static final String RESULTS = "../results.csv";
    private static final String README = "../README.md";

    private static final int ROWS = 500_000_000;

    public static void main(final String[] args) throws Exception {

        // Generate input
        Generate.createMeasurementsIfNotExists(ROWS, INPUT);

        // Run Benchmark against baseline:
        System.out.println("Starting baseline benchmark...");
        final long baseline = Benchmark.run(Calculate_baseline::run, INPUT, EXPECTED);
        System.out.printf("Baseline benchmark took %,dms%n", baseline);

        // Run Benchmark against current:
        System.out.println("Starting benchmark...");
        final long elapsed = Benchmark.run(CalculateAverage::run, INPUT, OUTPUT);
        System.out.printf("Benchmark took %,dms%n", elapsed);

        // Validate the output is correct
        Validate.compare(OUTPUT, EXPECTED);

        // Write results
        System.out.println("Getting Git commit information...");
        final String commitHash = Git.commitHash();
        final String commitMsg = Git.commitMessage();
        System.out.printf("Writing results to %s...%n", RESULTS);
        final Result.Row row = new Result.Row(commitHash, commitMsg, elapsed, baseline);
        Files.writeString(Paths.get(RESULTS), row.toString(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        Result.updateReadmeFromCSV(RESULTS, README);
    }



}

