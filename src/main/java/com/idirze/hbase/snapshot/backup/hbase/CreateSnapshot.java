package com.idirze.hbase.snapshot.backup.hbase;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Connection;

import static org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type.FLUSH;

@Slf4j
public class CreateSnapshot extends Configured implements BackupRestoreOperation {

    private String tableName;
    private String tableSnapshotId;
    private Connection connection;

    public CreateSnapshot(Connection connection, String tableName, String backupId) {
        this.connection = connection;
        this.tableName = tableName;
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, tableName);

    }

    @Override
    public void execute() throws Exception {
        log.info("Create snapshot: {} for table: {}", tableSnapshotId, tableName);
        SnapshotBackupUtils.createSnapshot(connection, tableName, tableSnapshotId, FLUSH.name());
    }

    @Override
    public void rollback() throws Exception {
        log.info("Rollback - Delete snapshot: {} for the table: {}", tableSnapshotId, tableName);
        SnapshotBackupUtils.deleteSnapshot(connection, tableSnapshotId);
    }
}
