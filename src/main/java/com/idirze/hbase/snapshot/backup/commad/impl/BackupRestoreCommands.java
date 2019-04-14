package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.CreateBackupOptions;
import com.idirze.hbase.snapshot.backup.cli.HistoryBackupOptions;
import com.idirze.hbase.snapshot.backup.cli.RestoreBackupOptions;
import com.idirze.hbase.snapshot.backup.cli.RollupBackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import com.idirze.hbase.snapshot.backup.commad.BackupId;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifest;
import com.idirze.hbase.snapshot.backup.manifest.BackupManifests;
import com.idirze.hbase.snapshot.backup.utils.FileUtils;
import com.idirze.hbase.snapshot.backup.utils.SnapshotBackupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.Tool;
import picocli.CommandLine;

@Slf4j
public class BackupRestoreCommands extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        String command = args.length == 0 ? "-h" : args[0];
        String backupManifestPath;
        BackupManifests backupManifest;
        int exit;
        switch (BackupCommand.backupCommandOf(command)) {
            case CREATE:
                try (final Connection conn = ConnectionFactory.createConnection(getConf())) {
                    CreateBackupOptions backupOpts = parseArgs(new CreateBackupOptions(), args);
                    BackupId bk = SnapshotBackupUtils.getOrCreateBackupId(conn);

                    String backupId = bk.getId();

                    return new BackupImpl(getConf())
                            .backup(conn, backupId, backupOpts);
                }

            case RESTORE:
                try (final Connection conn = ConnectionFactory.createConnection(getConf())) {

                    RestoreBackupOptions restoreOpts = parseArgs(new RestoreBackupOptions(), args);

                    return new RestoreImpl(getConf())
                            .restore(conn, restoreOpts);
                }


            case HISTORY:
                HistoryBackupOptions historyOpts = parseArgs(new HistoryBackupOptions(), args);
                return new HbaseBackupRestoreTable()
                        .addOperation(new HistoryBackup(getConf(), historyOpts))
                        .execute();

            case ROLLUP:
                RollupBackupOptions rollupBackupOpts = parseArgs(new RollupBackupOptions(), args);
                backupManifestPath = FileUtils.path(rollupBackupOpts.getBackupRooPath(), BackupManifest.BACKUP_MANIFEST_NAME);
                backupManifest = BackupManifests
                        .readFrom(getConf(), backupManifestPath);
                return new HbaseBackupRestoreTable()
                        .addOperation(new RollupBackup(getConf(), backupManifest, rollupBackupOpts))
                        .execute();

            default:
                return new HbaseBackupRestoreTable()
                        .addOperation(new HelpBackup(getConf()))
                        .execute();

        }


    }


    private <T> T parseArgs(T options, String[] args) {
        CommandLine cli = new CommandLine(options)
                .setCaseInsensitiveEnumValuesAllowed(true);

        boolean printUsage = false;

        try {
            cli.parse(args);
        } catch (Exception e) {
            printUsage = true;
        }

        if (printUsage || cli.isUsageHelpRequested()) {
            cli.usage(System.out);
            System.exit(0);
        }

        return options;
    }

}
