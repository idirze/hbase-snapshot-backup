package com.idirze.hbase.snapshot.backup.hbase;

import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

@Slf4j
public class RestoreSnapshot extends Configured implements BackupRestoreCommand {

    private String tableSnapshotId;
    private String table;

    public RestoreSnapshot(Configuration conf, String table, String backupId) {
        setConf(conf);
        this.tableSnapshotId = SnapshotBackupUtils.tableSnapshotId(backupId, table);
        this.table = table;
    }

    @Override
    public void execute() throws Exception {
        log.info("Restore snapshot: {}, for table: {}", tableSnapshotId, table);
        SnapshotBackupUtils.restoreSnapshot(getConf(), tableSnapshotId);
    }

    @Override
    public void rollback() throws Exception {

    }
}
