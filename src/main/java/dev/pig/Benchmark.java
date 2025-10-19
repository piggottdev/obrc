package dev.pig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Benchmark {

    public static void main(final String[] args) throws Exception {

        // Generate input data if not present
        final Path input = Paths.get("measurements.txt");
        if (!Files.exists(input)) {
            System.out.println("Generating input file...");
            CreateMeasurementsFast.main(new String[]{"500000000"});
        } else {
            System.out.println("Using existing input file " + input);
        }

        // Run the benchmark
        long start = System.currentTimeMillis();
        CalculateAverage.run();
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        final String result = (elapsed/60000) + "m " + ((elapsed/1000)%60) + "s " + (elapsed%1000) + " ms";

        // Save the benchmark results
        System.out.println("Saving benchmark results to results.csv");
        Files.writeString(
                Paths.get("results.csv"),
                result
        );
    }

}
