package com.idirze.hbase.snapshot.backup.hbase;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

@Slf4j
public class DisableTable extends Configured implements BackupRestoreCommand {


    private String tableName;

    public DisableTable(Configuration conf, String tableName) {
        setConf(conf);
        this.tableName = tableName;
    }

    @Override
    public void execute() throws Exception {
        log.info("Disable table: {}", tableName);
        SnapshotBackupUtils.disableTable(getConf(), tableName);
    }

    @Override
    public void rollback() throws Exception {
        log.info("Rollback - Enable table: {}", tableName);
        SnapshotBackupUtils.enableTable(getConf(), tableName);
    }
}
