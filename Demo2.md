### Clean UP

disable_all 'ns.*'
drop_all 'ns.*'

rm -fr /tmp/hbase_snapshot_backup && mkdir /tmp/hbase_snapshot_backup
alias ll='ls -ltr'


{ID=backup_1549515889703,Type=INCREMENTAL,Tables={ns1:t1,ns1:t2},State=COMPLETE,Start time=Thu Feb 07 05:04:50 UTC 2019,End time=Thu Feb 07 05:05:05 UTC 2019,Progress=100%} 

{ID=backup_1549515089475,Type=INCREMENTAL,Tables={ns1:t1,ns1:t2},State=COMPLETE,Start time=Thu Feb 07 04:51:30 UTC 2019,End time=Thu Feb 07 04:51:49 UTC 2019,Progress=100%} 

{ID=backup_1549514988719,Type=FULL,Tables={ns1:t1,ns1:t2},State=COMPLETE,Start time=Thu Feb 07 04:49:50 UTC 2019,End time=Thu Feb 07 04:50:02 UTC 2019,Progress=100%}

### Add data
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t1  full0 2
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t2  full0 4
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t3  full0 6
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t4  full0 8


### Create a backup for the whole namespace
hbase backup2 -Dmapreduce.framework.name=local create  -namespaces "ns1" \
      -backup_root_path file:///tmp/hbase_snapshot_backup

hbase backup2 history \
      -backup_root_path file:///tmp/hbase_snapshot_backup


### Add data on ns1:t1 and create a backup
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t1  inc01 2

hbase backup2 -Dmapreduce.framework.name=local create  -namespaces "ns1" \
      -backup_root_path file:///tmp/hbase_snapshot_backup

hbase backup2 history \
      -backup_root_path file:///tmp/hbase_snapshot_backup


### Add data on ns1:t2 and create a backup
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t2  inc02 2

hbase backup2 -Dmapreduce.framework.name=local create  -namespaces "ns1" \
      -backup_root_path file:///tmp/hbase_snapshot_backup

hbase backup2 history \
      -backup_root_path file:///tmp/hbase_snapshot_backup

### Restore inc01

sudo -u hbase hbase backup2 -Dmapreduce.framework.name=local restore  -backup_root_path file:///tmp/hbase_snapshot_backup \
-backup_id its_backup_1549523726949
     

### Restore to another cluster

sudo -u hbase hbase backup2 -Dmapreduce.framework.name=local restore  \
-backup_root_path file:///tmp/hbase_snapshot_backup  \
-restore_root_path  hdfs://sandbox-hdp.hortonworks.com:8020/apps/hbase/data \
-backup_id its_backup_1549523726949

### Rollup

hbase backup2 history  -backup_root_path file:///tmp/hbase_snapshot_backup
hbase backup2 rollup -backup_root_path file:///tmp/hbase_snapshot_backup -nb_backups 2

Or while creating the backups:

hbase backup2 create  -namespaces "ns1" \
      -backup_root_path file:///tmp/hbase_snapshot_backup -rollup 2


### Dynamique rajouter des tables ds le namespace














