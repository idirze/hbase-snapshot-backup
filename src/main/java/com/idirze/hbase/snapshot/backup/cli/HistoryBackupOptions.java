package com.idirze.hbase.snapshot.backup.cli;

import com.idirze.hbase.snapshot.backup.commad.BackupCommand;
import lombok.Data;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "history",
        mixinStandardHelpOptions = true,
        description = "Show history of all successful backups",
        showDefaultValues = true)
@Data
public class HistoryBackupOptions {

    @Parameters(index = "0",
            hidden = true)
    private BackupCommand command;

    @Option(names = "-backup_root_path",
            description = "The full root path to store the backup image: hdfs|file:///path/to/backup/dir",
            required = true)
    private String backupRooPath;

    @Option(names = "-n",
            description = "Show n latest backups",
            required = false,
            defaultValue = "0")
    private int n;
}


