package com.idirze.hbase.snapshot.backup.hbase;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

@Slf4j
public class DeleteSnapshot extends Configured implements BackupRestoreCommand {

    private String tableSnapshotId;
    private String tableName;

    public DeleteSnapshot(Configuration conf, String tableName, String backupId) {
        setConf(conf);
        this.tableName = tableName;
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);

    }

    @Override
    public void execute() throws Exception {
        log.info("Delete snapshot: {} for table: {}", tableSnapshotId, tableName);
        SnapshotBackupUtils.deleteSnapshot(getConf(), tableSnapshotId);
    }

    @Override
    public void rollback() {

    }
}
