package dev.pig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Validate {

    private static final String EXPECTED = "./measurements.out";

    public static void main(final String[] args) {

        try {
            final String result = CalculateAverage.run();
            System.out.println(Files.readString(Path.of(EXPECTED)).equals(result) ? "PASS" : "FAIL");
        } catch (IOException ioException) {
            System.out.println("FAILED: IOException:");
            ioException.printStackTrace();
        }

    }

}
