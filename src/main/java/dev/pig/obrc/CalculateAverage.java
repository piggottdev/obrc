package dev.pig.obrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

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

        final Map<String, Entry> stations = new HashMap<>();

        while (chunk.position() < chunk.capacity()) {

            // Mark the start of the line
            final int nameStart = chunk.position();
            // Move the position to one after the semicolon
            while (chunk.get() != ';') {}
            // Grab the name into a byte slice
            final byte[] name =  new byte[(chunk.position()-1) - nameStart];
            chunk.get(nameStart, name);

            // Mark the start of the temperature reading
            final int tempStart = chunk.position();
            // Move the position to one after the newline
            while (chunk.get() != '\n') {}
            // Grab the temperature into a byte slice
            final byte[] temp =  new byte[(chunk.position()-1) - tempStart];
            chunk.get(tempStart, temp);

            stations.computeIfAbsent(new String(name), k -> new Entry()).add(Double.parseDouble(new String(temp)));
        }

        return stations;
    }

    private static class Entry {

        private long count = 0;
        private double sum = 0;
        private double max = Double.NEGATIVE_INFINITY;
        private double min = Double.POSITIVE_INFINITY;

        private Entry add(final double val) {
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
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private static double round(final double val) {
            return Math.round(val * 10.0) / 10.0;
        }

    }

}

