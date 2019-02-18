package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.RestoreBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;

import java.net.URI;

@Slf4j
public class RestoreBackup extends Configured implements BackupRestoreCommand {

    private RestoreBackupOptions options;
    private String backupTableId;
    private String table;

    public RestoreBackup(Configuration conf, String tableName, String backupId, RestoreBackupOptions options) {
        super(conf);
        this.options = options;
        this.backupTableId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);
        this.table = tableName;
    }

    @Override
    public void execute() throws Exception {

        ExportSnapshot exportSnapshot = new ExportSnapshot(backupTableId, options.isSkipTmp());
        exportSnapshot.setConf(getConf());
        options.setInputRootPath(options.getBackupRooPath());
        // options.setBackupRooPath("hdfs:///apps/hbase/data");

        URI hbaseUri;
        if (options.getRestoreRooPath() != null) {
            // Quick chek
            if (options.getRestoreRooPath().indexOf("/apps/hbase/data") < 0) {
                log.error("Invalid restore path: {}, should be {}", options.getRestoreRooPath(), "hdfs://nameNode:port/apps/hbase/data");
                System.exit(-1);
            }
            hbaseUri = new Path(options.getRestoreRooPath()).toUri();
        } else {
            hbaseUri = FSUtils.getRootDir(getConf()).toUri();
        }

        options.setOutputRootPath(hbaseUri.getScheme() + "://" + hbaseUri.getPath());

        SnapshotBackupUtils.createNamespaceIfNotExistsForTable(getConf(), table);
        exportSnapshot.execute(options);
    }

    @Override
    public void rollback() {

    }

}
