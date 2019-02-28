package com.idirze.hbase.snapshot.backup.commad.impl;

import com.google.common.collect.Sets;
import com.idirze.hbase.snapshot.backup.cli.RollupBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo.Type;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.difference;
import static com.idirze.hbase.snapshot.backup.commad.BackupCommand.ROLLUP;
import static com.idirze.hbase.snapshot.backup.commad.BackupStatus.FAILED;
import static com.idirze.hbase.snapshot.backup.utils.FileUtils.path;


@Slf4j
public class RollupBackup extends Configured implements BackupRestoreOperation {

    private BackupManifests backupManifest;
    private RollupBackupOptions options;

    public RollupBackup(Configuration conf, BackupManifests backupManifest, RollupBackupOptions options) {
        super(conf);
        this.options = options;
        this.backupManifest = backupManifest;
    }

    private static Set<String> getSnapshotManifests() {
        return new HashSet<>();
    }

    @Override
    public void execute() throws Exception {

        log.info("Roll out backups");


        Pair<List<BackupManifest>, List<BackupManifest>> pair = backupManifest.rollup(options.getNbBackups());

        Set<String> removeSnapshotFiles = getSnapshotFiles(pair.getFirst());
        Set<String> keepSnapshotFiles = getSnapshotFiles(pair.getSecond());

        Sets.SetView<String> diff = difference(removeSnapshotFiles, keepSnapshotFiles);

        diff.stream()
                .map(f -> new Path(f))
                .forEach(p -> {
                    try {
                        FileSystem fs = FileSystem.get(p.toUri(), getConf());
                        if (fs.exists(p)) {
                            log.info("Flag the file: {} as deleted", p);
                            try {
                                //fs.delete(p, false);
                                fs.rename(p, new Path(p.toUri().getPath() + ".deleted"));
                            } catch (IOException e) {
                                //fs.delete(p, false);
                                fs.rename(p, new Path(p.toUri().getPath() + ".deleted"));
                            }
                        }
                    } catch (IOException e) {
                        log.error(e.getMessage(), e);
                    }

                });


        Set<BackupManifest> removeAll = new HashSet<>();
        pair.getFirst().forEach(e -> removeAll.add(e));
        backupManifest.getManifests().removeAll(removeAll);

        backupManifest.write();

    }

    @Override
    public void rollback() throws IOException {
        backupManifest
                .getManifests()
                .stream()
                .forEach(bm -> bm
                        .withCommand(ROLLUP)
                        .withBackupStatus(FAILED));
        backupManifest.write();
    }

    private Set<String> getSnapshotFiles(List<BackupManifest> manifests) throws IOException {
        Set<String> files = new HashSet<>();
        for (BackupManifest bm : manifests) {
            for (String table : bm.getTables()) {
                Path snapshotTableDir = new Path(path(options.getBackupRooPath(), ".hbase-snapshot/" + bm.getBackupId() + "_" + table.replace(":", "_")));
                FileSystem fs = FileSystem.get(snapshotTableDir.toUri(), getConf());
                List<Triple<String, Long, Type>> snapshotFiles = SnapshotBackupUtils
                        .getSnapshotFiles(getConf(), fs, snapshotTableDir);

                for (Triple<String, Long, Type> snapshotFile : snapshotFiles) {
                    files.add(path(options.getBackupRooPath() + "/archive/data", snapshotFile.getFirst()));
                }

            }
        }

        return files;
    }


}
