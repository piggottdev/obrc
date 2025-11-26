package dev.pig.obrc.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Validate {

    private static final String ACTUAL = "./results.out";
    private static final String EXPECTED = "./results_baseline.out";

    public static void main(final String[] args) throws IOException {

        final String actual = args.length >= 1 ? args[0] : ACTUAL;
        final String expected = args.length >= 2 ? args[1] : EXPECTED;

        compare(actual, expected);
    }

    static void compare(final String actual, final String expected) throws IOException {
        compare(Path.of(actual), Path.of(expected));
    }

    static void compare(final Path actual, final Path expected) throws IOException {
        if (!Files.readString(expected).equals(Files.readString(actual))) {
            System.err.printf("File Validation FAIL: %s is not equal to file %s%n", actual, expected);
            System.exit(1);
        }
        System.out.println("File Validation PASS");
    }

}