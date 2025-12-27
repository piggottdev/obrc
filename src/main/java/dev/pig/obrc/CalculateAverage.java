package dev.pig.obrc;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.function.BiConsumer;

public class CalculateAverage {

    private static final String INPUT = "./measurements.txt";

    public static void main(final String[] args) throws IOException {
        final String input = args.length >= 1 ? args[0] : INPUT;

        System.out.println(run(input));
    }

    public static String run(final String input) throws IOException {
        final List<StationArrayMap> chunks = chunkify(input).parallelStream()
                .map(CalculateAverage::processChunk)
                .toList();

        for (int i = 1; i < chunks.size(); i++) {
            chunks.getFirst().merge(chunks.get(i));
        }
        final TreeMap<ByteSpan, Station> sorted = new TreeMap<>();
        chunks.getFirst().forEach(sorted::put);

        return sorted.toString();
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

        final StationArrayMap stations = new StationArrayMap(8192);

        long address = baseAddress(chunk);
        long capacity = address + chunk.capacity();

        while (address < capacity) {

            // Parse the station name - UTF8 string, delimited by ;

            // Mark the start of the line
            final long lineStart = address;

            // Find the semicolon
            address = address - 8;
            int pos;
            do {
                address += 8;
                long x = UNSAFE.getLong(address) ^ 0x3B3B3B3B3B3B3B3BL;
                long mask = ((x - 0x0101010101010101L) & ~x) & 0x8080808080808080L;
                pos = Long.numberOfTrailingZeros(mask) >> 3; // 8 if no semicolon
            } while (pos == 8);
            address = address + pos;

            // Grab the name into a byte slice
            final ByteSpan name = new ByteSpan(lineStart, (int) (address-(lineStart)));


            // Parse the temperature - reading can be negative, 1 or 2 integer digits, 1 DP

            // Mark the start of the temperature reading
            final long tempStart = address+1;
            // Check if first character is (1 for negative, 0 for positive)
            final int negative = ~(UNSAFE.getByte(tempStart) >> 4) & 1;
            // Check how many integer digits there are (1 for 2 digits, 0 for 1 digit)
            final int isThree = ~(UNSAFE.getByte(tempStart+negative+2) >> 4) & 1;

            // Find the 3 digits (if there are only 2, d1 == d2)
            final int d1 = UNSAFE.getByte(tempStart + negative) - 48;
            final int d2 = UNSAFE.getByte(tempStart + negative + isThree);
            final int d3 = UNSAFE.getByte(tempStart + negative + isThree + 2);

            // Calculate temp from 3 digits
            final int temp = -negative ^ (d1*100*isThree + d2*10 + d3 - 528) - negative;

            // Add reading to map
            stations.getOrCreate(name).add(temp);

            // Progress head to next line start
            address = tempStart + negative + isThree + 4;
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

        private StationArrayMap(final int capacity) {
            this.mask = capacity - 1;
            this.keys = new ByteSpan[capacity];
            this.values = new Station[capacity];
        }

        private Station getOrCreate(final ByteSpan k) {
            int b = (k.hash ^ (k.hash >> 13) ^ (k.hash >> 16)) & this.mask;

            ByteSpan e = this.keys[b];
            while (e != null && (e.hash != k.hash || !e.equals(k))) {
                b = (b+1) & this.mask;
                e = this.keys[b];
            }
            if (e == null) {
                this.keys[b] = k;
                this.values[b] = new Station();
            }

            return this.values[b];
        }

        private void merge(final StationArrayMap other) {
            other.forEach((k, v) -> this.getOrCreate(k).merge(v));
        }

        private void forEach(final BiConsumer<ByteSpan, Station> consumer) {
            for (int i = 0; i < this.keys.length; i++) {
                if (this.keys[i] != null) {
                    consumer.accept(this.keys[i], this.values[i]);
                }
            }
        }
    }

    // -------------------------------------------------------------------
    // Byte Span
    // -------------------------------------------------------------------

    private static class ByteSpan implements Comparable<ByteSpan> {
        private final long address;
        private final int length;
        private final int hash;
        private String str;

        private ByteSpan(final long address, final int length) {
            this.address = address;
            this.length = length;
            if (this.length >= 4) {
                this.hash = UNSAFE.getInt(this.address);
            } else {
                this.hash = UNSAFE.getShort(this.address);
            }
        }

        public boolean equals(final ByteSpan span) {
            if (this.length != span.length) {
                return false;
            }

            int i = 0;
            while (i < this.length) {

                if (this.length - i >= 7) {
                    if (UNSAFE.getLong(this.address+i) != UNSAFE.getLong(span.address+i)) {
                        return false;
                    }
                    i += 8;
                    continue;
                }

                if (this.length - i >= 3) {
                    if (UNSAFE.getInt(this.address+i) != UNSAFE.getInt(span.address+i)) {
                        return false;
                    }
                    i += 4;
                    continue;
                }

                if (UNSAFE.getShort(this.address+i) != UNSAFE.getShort(span.address+i)) {
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
                UNSAFE.copyMemory(null, this.address, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, this.length);
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

        private void merge(final Station other) {
            this.count += other.count;
            this.sum += other.sum;
            this.max = Math.max(max, other.max);
            this.min = Math.min(min, other.min);
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

    // -------------------------------------------------------------------
    // Unsafe
    // -------------------------------------------------------------------

    private final static Unsafe UNSAFE = getUnsafe();

    private static Unsafe getUnsafe() {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static long baseAddress(final MappedByteBuffer mbb) {
        try {
            final Field addressF = Buffer.class.getDeclaredField("address");
            addressF.setAccessible(true);
            return addressF.getLong(mbb);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}

