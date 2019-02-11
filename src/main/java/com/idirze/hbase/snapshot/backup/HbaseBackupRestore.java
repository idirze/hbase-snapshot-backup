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

       /* conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        conf.addResource(new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml"));
        conf.addResource(new Path("/usr/hdp/current/hbase-client/conf/hbase-site.xml"));
*/

        System.exit(ToolRunner.run(conf, new BackupRestoreCommandImpl(conf), args));

    }


}
