package com.idirze.hbase.snapshot.backup.commad.impl;

import com.idirze.hbase.snapshot.backup.cli.BackupOptions;
import com.idirze.hbase.snapshot.backup.commad.BackupRestoreCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

@Slf4j
public class DeleteBackup extends Configured implements BackupRestoreCommand {

    private BackupOptions options;

    public DeleteBackup(Configuration conf, BackupOptions options) {
        setConf(conf);
        this.options = options;
    }


    @Override
    public void execute() {

    }

    @Override
    public void rollback(){
    }
}
