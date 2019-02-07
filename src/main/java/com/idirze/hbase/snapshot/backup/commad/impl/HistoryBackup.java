package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.HistoryBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import static com.idirze.hbase.snapshot.backup.manifest.BackupManifest.BACKUP_MANIFEST_NAME;
import static com.idirze.hbase.snapshot.backup.utils.FileUtils.path;

public class HistoryBackup extends Configured implements BackupRestoreCommand {

    private HistoryBackupOptions options;

    public HistoryBackup(Configuration conf, HistoryBackupOptions options) {
        setConf(conf);
        this.options = options;
    }

    @Override
    public void execute() throws Exception {
        String backupManifestPath = path(options.getBackupRooPath(), BACKUP_MANIFEST_NAME);

        BackupManifests
                .readFrom(getConf(),backupManifestPath)
                .showHistory(options.getN());
    }

    @Override
    public void rollback() {

    }
}
