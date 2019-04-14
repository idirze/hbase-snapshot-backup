package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.admin.DeleteSnapshot;
import com.idirze.hbase.snapshot.backup.admin.DisableTable;
import com.idirze.hbase.snapshot.backup.admin.EnableTable;
import com.idirze.hbase.snapshot.backup.admin.RestoreSnapshot;
import com.idirze.hbase.snapshot.backup.cli.RestoreBackupOptions;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class RestoreImpl extends Configured {

    public RestoreImpl(Configuration conf) {
        setConf(conf);
    }

    public int restore(Connection conn,
                       RestoreBackupOptions restoreOpts) throws Exception {

        int exit;
        String backupManifestPath = FileUtils
                .path(restoreOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);

        Map<String, String> tables;
        if (!Optional.ofNullable(restoreOpts.getTableMapping()).isPresent()) {


            BackupManifests backupManifest = BackupManifests.readFrom(getConf(), backupManifestPath);
            tables = backupManifest
                    .getManifests()
                    .stream()
                    .filter(m -> m.getBackupId().equals(restoreOpts.getBackupId()))
                    .flatMap(m -> m.getTables().stream())
                    .collect(Collectors.toMap(x -> x, x -> x));

            log.info("Restoring tables: {}", tables);

        } else {
            tables = restoreOpts.getTableMapping();
            log.info("Restoring to target tables: {}", tables);
        }


        for (Map.Entry<String, String> table : tables.entrySet()) {
            HbaseBackupRestoreTable bkRestoreTable = new HbaseBackupRestoreTable();
            log.info("Restoring table {} to {}", table.getKey(), table.getValue());
            bkRestoreTable
                    .addOperation(new RestoreBackup(conn, getConf(), table.getKey(), restoreOpts.getBackupId(), restoreOpts))
                    .addOperation(new DisableTable(conn, table.getValue()))
                    .addOperation(new RestoreSnapshot(conn, table.getKey(), table.getValue(), restoreOpts.getBackupId()))
                    .addOperation(new EnableTable(conn, table.getValue()))
                    .addOperation(new DeleteSnapshot(conn, table.getKey(), restoreOpts.getBackupId()));

            exit = bkRestoreTable.execute();
            if (exit != 0) return exit;
        }

        return 0;
    }
}
