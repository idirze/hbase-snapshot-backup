package com.idirze.hbase.snapshot.backup.cli;

import lombok.Data;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(name = "create",
        mixinStandardHelpOptions = true,
        description = "Create a new backup image",
        showDefaultValues = true)
@Data
public class CreateBackupOptions extends BackupOptions {

    @CommandLine.Option(names = "-skip_tmp",
            description = "Skip HDFS /tmp directory",
            required = false,
            defaultValue = "false",
            showDefaultValue = ALWAYS)
    private boolean skipTmp;


    @CommandLine.Option(names = "-rollout",
            description = "Number of backup to maintains",
            required = false,
            defaultValue = "-1")
    private int rollout;


}


