package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.CreateBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.commad.BackupStatus;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.FileUtils;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.FSUtils;
import org.joda.time.DateTime;

import java.net.URI;

@Slf4j
public class CreateBackup extends Configured implements BackupRestoreCommand {

    private String tableName;
    private String backupId;
    private String tableSnapshotId;
    private BackupManifests backupManifest;
    private CreateBackupOptions options;


    public CreateBackup(Configuration conf, String tableName, String backupId, BackupManifests backupManifest, CreateBackupOptions options) {
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
        ExportSnapshot exportSnapshot = new ExportSnapshot(tableSnapshotId, options.isSkipTmp());
        exportSnapshot.setConf(getConf());
        // options.setInputRootPath("hdfs:///apps/hbase/data");
        URI hbaseUri = FSUtils.getRootDir(getConf()).toUri();
        options.setInputRootPath(hbaseUri.getScheme() + "://" + hbaseUri.getPath());

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
