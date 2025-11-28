package dev.pig.obrc.pipeline;

import dev.pig.obrc.CalculateAverage;

public class Runner {

    private static final String INPUT = "./measurements.txt";
    private static final String OUTPUT = "./results.out";
    private static final String EXPECTED = "./results_baseline.out";
    private static final String RESULTS = "./results.csv";

    private static final int ROWS = 300_000_000;

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
        final String commitHash = Git.commitHash();
        final String commitMsg = Git.commitMessage();
        final ResultRow row = new ResultRow(commitHash, commitMsg, elapsed, baseline);
        System.out.println(row);
        // - Append to CSV file
    }

    private static class ResultRow {

        private final String commit;
        private final String description;
        private final double runtime;
        private final double difference;
        private final double improvement;

        private ResultRow(final String commit, final String description, final long runtime, final long baseline) {
            this.commit = commit;
            this.description = description;
            this.runtime = runtime / 1000.0;
            this.difference = this.runtime - (baseline / 1000.0);
            this.improvement =  (1.0 - (runtime*1.0)/baseline) * 100.0;
        }

        public String toString() {
            return String.format("%s,%s,%.3fs,%.3fs,%.2f%%%n",this.commit, this.description, this.runtime, this.difference, this.improvement);
        }

    }

}

