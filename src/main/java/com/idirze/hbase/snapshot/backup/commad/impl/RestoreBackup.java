package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.RestoreBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.FSUtils;

import java.net.URI;

import static com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils.tableSnapshotId;

@Slf4j
public class RestoreBackup extends Configured implements BackupRestoreOperation {

    private RestoreBackupOptions options;
    private String backupTableId;
    private String table;
    private Connection connection;

    public RestoreBackup(Connection connection, Configuration conf, String tableName, String backupId, RestoreBackupOptions options) {
        super(conf);
        this.options = options;
        this.backupTableId = tableSnapshotId(backupId, tableName);
        this.table = tableName;
        this.connection = connection;

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

        exportSnapshot.execute(options);
    }

    @Override
    public void rollback() {

    }

}
