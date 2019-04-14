package com.idirze.hbase.snapshot.backup.stats;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;


@Slf4j
public class Stats {

    static DateTimeFormatter fmt =
            DateTimeFormatter.ofLocalizedDateTime(FormatStyle.FULL)
                    .withLocale(Locale.getDefault())
                    .withZone(ZoneId.systemDefault());

    private static List<Stat> stats = new ArrayList<>();

    public synchronized static void add(Stat stat) {
        stats.add(stat);
    }


    public static void log(Stat stat) {
        log.info("The create snapshot {} for table {} take {} seconds (start: {}, end: {})",
                stat.getId(),
                stat.getTable(),
                stat.elapsedTimeSeconds(),
                fmt.format(stat.getStartTime()),
                fmt.format(stat.getEndTime()));
    }

    public static void log() {
        log.info(" ======= SUMMARY ======== ");
        stats
                .stream()
                .forEach(s -> log(s));
        logAgg();
        log.info(" ==================== ");
    }


    public static void logAgg() {
        Aggregate agg = new Aggregate();
        log.info("The total time taken to create snapshots for all the tables is {} seconds ({} minutes) [start: {}, end: {}]",
                agg.elapsed.getSeconds(),
                agg.elapsed.getSeconds() / 60,
                fmt.format(agg.start),
                fmt.format(agg.end));
    }

    @Data
    @AllArgsConstructor
    public static class Stat {
        private String id;
        private String table;
        private StatType type;
        private Instant startTime;
        private Instant endTime;

        public long elapsedTimeSeconds() {
            return Duration.between(startTime, endTime).getSeconds();
        }
    }

    @Data
    static class Aggregate {

        private Instant start;
        private Instant end;
        private Duration elapsed;

        Aggregate() {
            this.start = Instant.ofEpochMilli(stats
                    .stream()
                    .mapToLong(s -> s.startTime.toEpochMilli())
                    .min()
                    .orElseThrow(NoSuchElementException::new));
            this.end = Instant.ofEpochMilli(stats
                    .stream()
                    .mapToLong(s -> s.endTime.toEpochMilli())
                    .max()
                    .orElseThrow(NoSuchElementException::new));

            this.elapsed = Duration.between(start, end);
        }

    }

    public enum StatType {
        CREATE_SNAPSHOT
    }
}
