package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.BackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.commad.BackupStatus;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.joda.time.DateTime;

@Slf4j
public class CreateBackup extends Configured implements BackupRestoreCommand {

    private String tableName;
    private String backupId;
    private String tableSnapshotId;
    private BackupManifests backupManifest;
    private BackupOptions options;


    public CreateBackup(Configuration conf, String tableName, String backupId, BackupManifests backupManifest, BackupOptions options) {
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
        ExportSnapshot exportSnapshot = new ExportSnapshot(tableSnapshotId);
        exportSnapshot.setConf(getConf());
        options.setInputRootPath("hdfs:///apps/hbase/data");
        //options.setInputRootPath(FSUtils.getRootDir(getConf()).toUri().getPath());
        exportSnapshot.execute(options);

        backupManifest
                .add(new BackupManifest()
                        .withBackupId(backupId)
                        .withDate(DateTime.now())
                        .withCommand(BackupCommand.CREATE)
                        .withBackupRootDir(options.getBackupRooPath())
                        .withBackupStatus(BackupStatus.SUCCESS)
                        .addTable(tableName))
                .write();
    }

    @Override
    public void rollback() throws Exception {
        log.error("rollback create backup - NA");
    }


}
