package com.idirze.hbase.snapshot.backup.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

@Slf4j
@ToString
@Data
public class BackupManifests {

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static String manifestPath;
    private static Configuration configuration;
    @Getter
    private Set<BackupManifest> manifests = new TreeSet<>(new SortByDate());

    public static BackupManifests readFrom(Configuration conf, String path) throws IOException {
        setPath(conf, path);
        String json;
        FSDataInputStream inputStream = null;
        FileSystem fs = null;
        try {
            log.info("Read backup manifest file from: {}", path);
            fs = FileSystem.get(new Path(path).toUri(), conf);
            inputStream = fs.open(new Path(path));
            //Classical input stream usage
            json = IOUtils.toString(inputStream, "UTF-8");
        } catch (FileNotFoundException e) {
            return new BackupManifests();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

        return new BackupManifests()
                .addAll(objectMapper.readValue(json, BackupManifests.class).getManifests());
    }

    private static synchronized void setPath(Configuration conf, String path) {
        configuration = conf;
        manifestPath = path;
    }

    public BackupManifests add(BackupManifest manifest) {
        manifests.add(manifest);
        return this;
    }

    public Optional<BackupManifest> findByTable(String backupId) {
        return manifests
                .stream()
                .filter(m -> m.getBackupId() != null && m.getBackupId().equals(backupId))
                .findFirst();
    }

    public void writeTo(Configuration conf, String path) throws IOException {

        FSDataOutputStream outputStream = null;
        FileSystem fs = null;
        Path parentDir = new Path(path).getParent();
        Path filePath = new Path(path);

        try {
            log.info("Update backup manifest file to: {}", path);
            fs = FileSystem.get(parentDir.toUri(), conf);
            if (!fs.exists(parentDir)) {
                fs.mkdirs(parentDir);
            }

            outputStream = fs.create(filePath);
            outputStream.writeBytes(objectMapper
                    .writeValueAsString(this));

        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

    }

    public void write() throws IOException {
        writeTo(configuration, manifestPath);
    }

    public Pair<List<BackupManifest>, List<BackupManifest>> rollup(Integer n) {
        Integer nbLast = checkCondition(n);
        return new Pair<>(new ArrayList<>(manifests).subList(0, manifests.size() - nbLast),
                new ArrayList<>(manifests).subList(manifests.size() - nbLast, manifests.size()));
    }

    public void showHistory(Integer n) {

        Integer nbLast = checkCondition(n);

        System.out.println("\n");
        System.out.println("BACKUP HISTORY - SIZE (" + manifests.size() + ")");
        System.out.println("---------------");

        for (BackupManifest manifest : new ArrayList<>(manifests).subList(manifests.size() - nbLast, manifests.size())) {
            System.out.println(manifest);
            System.out.println("-----------------------------------");
        }

    }

    private int checkCondition(int n) {
        if (n == 0 || n > manifests.size()) {
            return manifests.size();
        }
        return n;
    }

    private BackupManifests addAll(Set<BackupManifest> manifests) {
        this.manifests.addAll(manifests);
        return this;
    }

    private static class SortByDate implements Comparator<BackupManifest> {
        public int compare(BackupManifest n1, BackupManifest n2) {
            return BackupManifest.pattern
                    .parseDateTime(n1.getDate())
                    .compareTo(BackupManifest.pattern.parseDateTime(n2.getDate()));
        }
    }

}
