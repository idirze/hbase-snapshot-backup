package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.CreateBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.FSUtils;

import java.net.URI;

import static com.idirze.hbase.snapshot.backup.commad.BackupStatus.SUCCESS;

@Slf4j
public class DoExportSnapshot extends Configured implements BackupRestoreOperation {

    private String tableName;
    private String backupId;
    private String tableSnapshotId;
    private BackupManifests backupManifest;
    private CreateBackupOptions options;
    private int nbRetry = 2;


    public DoExportSnapshot(Configuration conf, String tableName, String backupId, BackupManifests backupManifest, CreateBackupOptions options) {
        setConf(conf);
        this.tableName = tableName;
        this.backupId = backupId;
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);

        this.backupManifest = backupManifest;

        this.options = options;
    }

    @Override
    public void execute() throws Exception {

        log.info("Export the snapshot: {} for table: {}", tableSnapshotId, tableName);

        boolean alreadyExported = backupManifest.getManifests()
                .stream()
                .filter(bm -> bm.getBackupId().equals(backupId) &&
                        bm.getTables().contains(tableName)
                        && bm.getStatus() == SUCCESS)
                .count() > 0;

        if (!alreadyExported) {
            ExportSnapshot exportSnapshot = new ExportSnapshot(tableSnapshotId, options.isSkipTmp());
            exportSnapshot.setConf(getConf());
            // options.setInputRootPath("hdfs:///apps/hbase/data");
            URI hbaseUri = FSUtils.getRootDir(getConf()).toUri();
            options.setInputRootPath(hbaseUri.getScheme() + "://" + hbaseUri.getPath());
            options.setOutputRootPath(options.getBackupRooPath());

            exportSnapshot.execute(options);

            BackupManifest bm = backupManifest.findByTable(backupId)
                    .orElse(new BackupManifest()
                            .withBackupId(backupId)
                            .withDate(SnapshotBackupUtils.getTimestamp(backupId))
                            .withCommand(BackupCommand.CREATE)
                            .withBackupRootDir(options.getBackupRooPath())
                            .withBackupStatus(SUCCESS));

            bm.addTable(tableName);

            backupManifest
                    .add(bm)
                    .write();
        } else {
            log.info("The table {} for snapshot {}/{} was already exported", tableName, backupId, tableSnapshotId);
        }
    }

    @Override
    public void rollback() throws Exception {
        log.error("Create backup failed - will retry {} times", nbRetry);
        if (nbRetry-- > 0) {
            execute();
        } else {
            log.error("Number of retries reached {}", nbRetry);
        }
    }


}
