package dev.pig;

public class Benchmark {

    public static void main(final String[] args) throws Exception {

        long start = System.currentTimeMillis();
        CalculateAverage_pigdev.main(args);
        long end = System.currentTimeMillis();

        long elapsed = end - start;
        System.out.println((elapsed/60000) + "m " + ((elapsed/1000)%60) + "s " + (elapsed%1000) + " ms");
    }

}
