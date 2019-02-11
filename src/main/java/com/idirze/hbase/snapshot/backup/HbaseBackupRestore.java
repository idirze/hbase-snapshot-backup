package com.idirze.hbase.snapshot.backup;

import com.idirze.hbase.snapshot.backup.commad.impl.BackupRestoreCommandImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;


@Slf4j
public class HbaseBackupRestore {

    public static void main(String[] args) throws Exception {

        final Configuration conf = HBaseConfiguration.create();

        System.exit(ToolRunner.run(conf, new BackupRestoreCommandImpl(conf), args));

    }


}
