package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.RestoreBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

@Slf4j
public class RestoreBackup extends Configured implements BackupRestoreCommand {

    private RestoreBackupOptions options;
    private String backupTableId;

    public RestoreBackup(Configuration conf, String tableName, String backupId, RestoreBackupOptions options) {
        super(conf);
        this.options = options;
        this.backupTableId =  SnapshotBackupUtils.tableSnapshotId(backupId, tableName);
    }

    @Override
    public void execute() throws Exception {

        ExportSnapshot exportSnapshot = new ExportSnapshot(backupTableId);
        exportSnapshot.setConf(getConf());
        options.setInputRootPath(options.getBackupRooPath());
        options.setBackupRooPath("hdfs:///apps/hbase/data");
        exportSnapshot.execute(options);

         exportSnapshot.execute(options);
    }

    @Override
    public void rollback() {

    }

}
