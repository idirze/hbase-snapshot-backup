package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.RestoreBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.util.FSUtils;

import java.net.URI;

@Slf4j
public class RestoreBackup extends Configured implements BackupRestoreCommand {

    private RestoreBackupOptions options;
    private String backupTableId;

    public RestoreBackup(Configuration conf, String tableName, String backupId, RestoreBackupOptions options) {
        super(conf);
        this.options = options;
        this.backupTableId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);
    }

    @Override
    public void execute() throws Exception {

        ExportSnapshot exportSnapshot = new ExportSnapshot(backupTableId, options.isSkipTmp());
        exportSnapshot.setConf(getConf());
        options.setInputRootPath(options.getBackupRooPath());
        // options.setBackupRooPath("hdfs:///apps/hbase/data");
        URI hbaseUri = FSUtils.getRootDir(getConf()).toUri();
        options.setBackupRooPath(hbaseUri.getScheme() + "://" + hbaseUri.getPath());

        exportSnapshot.execute(options);
    }

    @Override
    public void rollback() {

    }

}
