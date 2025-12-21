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
        final Map<ByteSpan, Station> result = chunkify(input).parallelStream()
                .flatMap(chunk -> processChunk(chunk).entryList().stream())
                .collect(Collectors.<Map.Entry<ByteSpan, Station>, ByteSpan, Station, TreeMap<ByteSpan, Station>>toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        Station::merge,
                        TreeMap::new
                ));

        return result.toString();
    }

    // chunkify takes a filename and splits it into a list of MappedByteBuffers.
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

    // processChunk process the chunk and returns a map of ByteSpan to Station.
    private static StationArrayMap processChunk(final MappedByteBuffer chunk) {

        final StationArrayMap stations = new StationArrayMap(16384);

        while (chunk.position() < chunk.capacity()) {

            // Parse the station name - UTF8 string, delimited by ;

            // Mark the start of the line
            final int nameStart = chunk.position();
            // Find the semicolon
            int pos = 8;
            while (pos == 8) {
                int x = chunk.getInt() ^ 0x3B3B3B3B;
                int mask = x - 0x01010101;
                mask = mask & ~x;
                mask = mask & 0x80808080;
                pos = Long.numberOfTrailingZeros(mask) >> 3; // 8 if no semicolon, or 0-3 inverse
            }
            chunk.position(chunk.position()-pos);

            // Grab the name into a byte slice
            final ByteSpan name = new ByteSpan(nameStart, chunk.position()-(nameStart+1), chunk);

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

            stations.getOrCreate(name).add(temp);
            chunk.position(tempStart + negative + isThree + 4);
        }

        return stations;
    }

    // -------------------------------------------------------------------
    // Station Array Map
    // -------------------------------------------------------------------

    private static class StationArrayMap {
        private final int mask;
        private final ByteSpan[] keys;
        private final Station[] values;

        private final ArrayList<Map.Entry<ByteSpan, Station>> entries;

        private StationArrayMap(final int capacity) {
            this.mask = capacity - 1;
            this.keys = new ByteSpan[capacity];
            this.values = new Station[capacity];
            this.entries = new ArrayList<>(capacity>>4);
        }

        private Station getOrCreate(final ByteSpan k) {
            int b = (k.hash ^ (k.hash >> 16)) & this.mask;

            ByteSpan e = this.keys[b];
            while (e != null && (e.hash != k.hash || !e.equals(k))) {
                b = (b+1) & this.mask;
                e = this.keys[b];
            }
            if (e == null) {
                this.keys[b] = k;
                this.values[b] = new Station();
                this.entries.add(Map.entry(k, this.values[b]));
            }

            return this.values[b];
        }

        private List<Map.Entry<ByteSpan, Station>> entryList() {
            return this.entries;
        }
    }

    // -------------------------------------------------------------------
    // Byte Span
    // -------------------------------------------------------------------

    private static class ByteSpan implements Comparable<ByteSpan> {
        private final int index;
        private final int length;
        private final MappedByteBuffer buffer;
        private final int hash;

        private String str;

        private ByteSpan(final int index, final int length, final MappedByteBuffer buffer) {
            this.index = index;
            this.length = length;
            this.buffer = buffer;
            if (this.length >= 4) {
                this.hash= this.buffer.getInt(this.index);
            } else {
                this.hash = this.buffer.getShort(this.index);
            }
        }

        public boolean equals(final ByteSpan span) {
            if (this.length != span.length) {
                return false;
            }

            int i = 0;
            while (i < this.length) {

                if (this.length - i >= 7) {
                    if (this.buffer.getLong(this.index+i) != span.buffer.getLong(span.index+i)) {
                        return false;
                    }
                    i += 8;
                    continue;
                }

                if (this.length - i >= 3) {
                    if (this.buffer.getInt(this.index+i) != span.buffer.getInt(span.index+i)) {
                        return false;
                    }
                    i += 4;
                    continue;
                }

                if (this.buffer.getShort(this.index+i) != span.buffer.getShort(span.index+i)) {
                    return false;
                }
                i += 2;
            }

            return true;
        }

        @Override
        public String toString() {
            if (this.str == null) {
                final byte[] bytes = new byte[this.length];
                this.buffer.get(this.index, bytes);
                this.str = new String(bytes);
            }
            return this.str;
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        @Override
        public int compareTo(final ByteSpan span) {
            return this.toString().compareTo(span.toString());
        }
    }

    // -------------------------------------------------------------------
    // Station
    // -------------------------------------------------------------------

    private static class Station {
        private int count = 0;
        private long sum = 0;
        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;

        private void add(final int val) {
            this.count++;
            this.sum += val;
            this.max = Math.max(this.max, val);
            this.min = Math.min(this.min, val);
        }

        private Station merge(final Station other) {
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

