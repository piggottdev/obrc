package dev.pig.obrc.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Result {

    private static final String TABLE_START = "<!-- RESULTS_START -->";
    private static final String TABLE_END = "<!-- RESULTS_END -->";

    static void updateReadmeFromCSV(final String csv, final String readme) throws IOException {
        updateReadmeFromCSV(Path.of(csv), Path.of(readme));
    }

    static void updateReadmeFromCSV(final Path csv, final Path readme) throws IOException {

        final StringBuilder md = new StringBuilder();
        md.append("| Commit | Message | Runtime | Difference | Improvement |\n");
        md.append("|--------|---------|---------|------------|-------------|\n");

        Files.readAllLines(csv).stream()
                .skip(1)
                .filter(l -> !l.isBlank())
                .map(Row::FromCSV)
                .forEach(r -> md.append("| ")
                        .append(r.commit).append(" | ")
                        .append(r.description).append(" | ")
                        .append(String.format("%.3fs", r.runtime)).append(" | ")
                        .append(String.format("%.3fs", r.difference)).append(" | ")
                        .append(String.format("%.2f%%", r.improvement)).append(" |\n"));

        final String oldReadme = Files.readString(readme);

        final int start = oldReadme.indexOf(TABLE_START);
        final int end = oldReadme.indexOf(TABLE_END);

        if (start == -1 || end == -1 || end < start) {
            throw new IllegalArgumentException("README missing table markers");
        }

        final String before = oldReadme.substring(0, start + TABLE_START.length());
        final String after = oldReadme.substring(end);

        final String newReadme = before + "\n\n" + md.toString().trim() + "\n\n" + after;

        System.out.println("Writing new markdown table to README");
        Files.writeString(readme, newReadme, StandardOpenOption.TRUNCATE_EXISTING);
    }

    static class Row {

        private final String commit;
        private final String description;
        private final double runtime;
        private final double difference;
        private final double improvement;

        Row(final String commit, final String description, final long runtime, final long baseline) {
            this.commit = commit;
            this.description = description;
            this.runtime = runtime / 1000.0;
            this.difference = this.runtime - (baseline / 1000.0);
            this.improvement =  (1.0 - (runtime*1.0)/baseline) * 100.0;
        }

        Row(final String commit, final String description, final double runtime, final double difference, final double improvement) {
            this.commit = commit;
            this.description = description;
            this.runtime = runtime;
            this.difference = difference;
            this.improvement = improvement;
        }

        static Row FromCSV(final String line) {
            final String[] fields = line.split(",");
            return new Row(
                    fields[0],
                    fields[1],
                    Double.parseDouble(fields[2].substring(0, fields[2].length()-1)),
                    Double.parseDouble(fields[3].substring(0, fields[3].length()-1)),
                    Double.parseDouble(fields[4].substring(0, fields[4].length()-1))
            );
        }

        public String toString() {
            return String.format("%s,%s,%.3fs,%.3fs,%.2f%%%n",this.commit, this.description, this.runtime, this.difference, this.improvement);
        }

    }
}
