package com.idirze.hbase.snapshot.backup.cli;


import picocli.CommandLine.Command;

@Command(mixinStandardHelpOptions = true,
        subcommands = {
                CreateBackupOptions.class,
                RestoreBackupOptions.class,
                HistoryBackupOptions.class,
                RollupBackupOptions.class
        })
public class MainCmdsOptions {
}
