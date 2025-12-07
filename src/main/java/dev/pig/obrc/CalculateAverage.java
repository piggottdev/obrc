package dev.pig.obrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

public class CalculateAverage {

    private static final String INPUT = "./measurements.txt";

    public static void main(final String[] args) throws IOException {

        final String input = args.length >= 1 ? args[0] : INPUT;

        System.out.println(run(input));
    }

    public static String run(final String input) throws IOException {

        final Map<String, Entry> result = chunkify(input).parallelStream()
                .flatMap(chunk -> processChunk(chunk).entrySet().stream())
                .collect(Collectors.<Map.Entry<String, Entry>, String, Entry, TreeMap<String, Entry>>toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        Entry::merge,
                        TreeMap::new
                ));

        return result.toString();
    }

    private static List<MappedByteBuffer> chunkify(final String filename) throws IOException {
        final RandomAccessFile file = new RandomAccessFile(filename, "r");

        final long processorCount = Runtime.getRuntime().availableProcessors();
        final long fileSize = file.length();
        final long chunkSize = Math.min(Integer.MAX_VALUE - 1000, fileSize / processorCount);
        final int chunkCount =  (int) (fileSize / chunkSize) + 1;

        final List<MappedByteBuffer> chunks = new ArrayList<>(chunkCount);

        long head = 0L;

        // For each chunk
        while (head < fileSize) {

            // Jump to the back
            long tail = head + chunkSize;
            // Find either the next new line character or the EOF
            if (tail > fileSize) {
                tail = fileSize;
            } else {
                file.seek(tail);
                while (file.read() != '\n') {
                    tail++;
                }
                tail++;
            }

            // Add the chunk to the list and set the head to the tail
            chunks.add(file.getChannel().map(FileChannel.MapMode.READ_ONLY, head, tail - head));
            head = tail;
        }

        return chunks;
    }

    private static Map<String, Entry> processChunk(final MappedByteBuffer chunk) {

        final Map<String, Entry> stations = new HashMap<>(500);

        while (chunk.position() < chunk.capacity()) {

            // Parse the station name - UTF8 string, delimited by ;

            // Mark the start of the line
            final int nameStart = chunk.position();
            // Move the position to one after the semicolon
            while (chunk.get() != ';') {}
            // Grab the name into a byte slice
            final byte[] name =  new byte[(chunk.position()-1) - nameStart];
            chunk.get(nameStart, name);

            // Parse the temperature - reading can be negative, 1 or 2 integer digits, 1 DP

            // Mark the start of the temperature reading
            final int tempStart = chunk.position();

            // Check if first character is (1 for negative, 0 for positive)
            final int negative = ~(chunk.get(tempStart) >> 4) & 1;
            // Check how many integer digits there are (1 for 2 digits, 0 for 1 digit)
            final int isThree = ~(chunk.get(tempStart+negative+2) >> 4) & 1;

            // Find the 3 digits (if there are only 2, d1 == d2)
            final int d1 = chunk.get(tempStart + negative) - 48;
            final int d2 = chunk.get(tempStart + negative + isThree);
            final int d3 = chunk.get(tempStart + negative + isThree + 2);

            // Calculate temp from 3 digits
            final int temp = -negative ^ (d1*100*isThree + d2*10 + d3 - 528) - negative;

            stations.computeIfAbsent(new String(name), k -> new Entry()).add(temp);
            chunk.position(tempStart + negative + isThree + 4);
        }

        return stations;
    }

    private static class Entry {

        private int count = 0;
        private long sum = 0;
        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;

        private Entry add(final int val) {
            this.count++;
            this.sum += val;
            this.max = Math.max(this.max, val);
            this.min = Math.min(this.min, val);
            return this;
        }

        private Entry merge(final Entry other) {
            this.count += other.count;
            this.sum += other.sum;
            this.max = Math.max(max, other.max);
            this.min = Math.min(min, other.min);
            return this;
        }

        @Override
        public String toString() {
            return round(min) + "/" + roundAverage(sum, count) + "/" + round(max);
        }

        private static double round(final int val) {
            return (double) (val) / 10.0;
        }

        private static double roundAverage(final long sum, final int count) {
            return Math.round((double) sum / (double) count) / 10.0;
        }

    }

}

