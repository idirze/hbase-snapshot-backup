package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.MainCmdsOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import picocli.CommandLine;

public class HelpBackup extends Configured implements BackupRestoreOperation {

    public HelpBackup(Configuration conf) {
        setConf(conf);
    }


    @Override
    public void execute() {
        CommandLine cli = new CommandLine(new MainCmdsOptions())
                .setCaseInsensitiveEnumValuesAllowed(true);

        cli.usage(System.out);
    }

    @Override
    public void rollback() {

    }
}
