package dev.pig.obrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

public class Current {

    private static final String INPUT = "./measurements.txt";

    public static void main(final String[] args) throws IOException {

        final String input = args.length >= 1 ? args[0] : INPUT;

        System.out.println(run(input));
    }

    public static String run(final String input) throws IOException {
        final RandomAccessFile file = new RandomAccessFile(input, "r");

        final Map<String, Entry> result = chunkify(file).parallelStream()
                .flatMap(buf -> Current.processChunk(buf).entrySet().stream())
                .collect(Collectors.<Map.Entry<String, Entry>, String, Entry, TreeMap<String, Entry>>toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        Entry::aggregate,
                        TreeMap::new
                ));

        return result.toString();
    }

    private static List<MappedByteBuffer> chunkify(final RandomAccessFile file) throws IOException {

        int chunkCount = Runtime.getRuntime().availableProcessors();

        final long fileSize = file.length();
        final long chunkSize = Math.min(Integer.MAX_VALUE - 1000, fileSize / chunkCount);
        chunkCount = ((int) (fileSize / chunkSize)) + 1;

        final List<MappedByteBuffer> chunks = new ArrayList<>(chunkCount);

        long head = 0L;
        while (head < fileSize) {
            long tail = head + chunkSize;
            if (tail > fileSize) {
                tail = fileSize;
            }
            else {
                file.seek(tail);
                while (file.read() != '\n') {
                    tail++;
                }
                tail++;
            }
            final MappedByteBuffer mbb = file.getChannel().map(FileChannel.MapMode.READ_ONLY, head, tail - head);
            chunks.add(mbb);
            head = tail;
        }

        return chunks;
    }

    private static Map<String, Entry> processChunk(final MappedByteBuffer chunk) {

        final Map<String, Entry> stations = new HashMap<>();

        int capacity = chunk.capacity();
        int mark = 0;

        while (mark != capacity) {

            int position = mark;

            // Find the semicolon, read the name station name
            while (chunk.get(++position) != ';') {}
            final byte[] name = new byte[position - mark];
            chunk.get(mark, name);

            // Move the mark to the first digit of the temperature
            position++;
            mark = position;

            // Parse the reading, knowing there is only 1DP and max 2 non-decimal digits
            double reading;
            boolean negative = chunk.get(position++) == '-';
            if (negative) {
                mark = position++;
                reading = -(chunk.get(mark) - 48);
                if (chunk.get(position) != '.') {
                    reading = (reading*10) - (chunk.get(position) - 48);
                    position++;
                }
                reading -= (double) (chunk.get(++position) - 48) /10;
            } else {
                reading = (chunk.get(mark) - 48);
                if (chunk.get(position) != '.') {
                    reading = (reading*10) + (chunk.get(position) - 48);
                    position++;
                }
                reading += (double) (chunk.get(++position) - 48) /10;
            }

            stations.computeIfAbsent(new String(name), k -> new Entry()).addReading(reading);

            mark = position + 2;
        }

        return stations;
    }

    private static class Entry {

        private long count = 0;
        private double sum = 0;
        private double max = Double.NEGATIVE_INFINITY;
        private double min = Double.POSITIVE_INFINITY;

        private void addReading(final double val) {
            count++;
            sum += val;
            max = Math.max(val, max);
            min = Math.min(val, min);
        }

        private Entry aggregate(final Entry other) {
            count += other.count;
            sum += other.sum;
            max = Math.max(max, other.max);
            min = Math.min(min, other.min);
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
