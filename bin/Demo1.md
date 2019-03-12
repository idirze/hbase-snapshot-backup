
### Create table

create_namespace 'ns1'
create 'ns1:t1', 'cf1'
create 'ns1:t2', 'cf1'
create 'ns1:t3', 'cf1'

### Put some data
put 'ns1:t1', 'key1', 'cf1:c1', 'value-1'
put 'ns1:t1', 'key2', 'cf1:c1', 'value-2'
put 'ns1:t1', 'key3', 'cf1:c1', 'value-3'

put 'ns1:t2', 'key1', 'cf1:c1', 'value-1'
put 'ns1:t2', 'key2', 'cf1:c1', 'value-2'
put 'ns1:t2', 'key3', 'cf1:c1', 'value-3'

put 'ns1:t3', 'key1', 'cf1:c1', 'value-1'
put 'ns1:t3', 'key2', 'cf1:c1', 'value-2'

### Create a backup for the whole namespace
hbase backup1 -Dmapreduce.framework.name=local  create -namespaces "ns1" \
              -backup_root_path file:///tmp/hbase_snapshot_backup

### You can also create backup for a set of tables only
hbase backup1 -Dmapreduce.framework.name=local create -tables "ns1:t1,ns1:t2" \
      -backup_root_path file:///tmp/hbase_snapshot_backup

### Add data and create another backup
put 'ns1:t1', 'key4', 'cf1:c1', 'value-4'
put 'ns1:t1', 'key5', 'cf1:c1', 'value-5'

hbase backup1 -Dmapreduce.framework.name=local create  -tables "ns1:t1" \
      -backup_root_path file:///tmp/hbase_snapshot_backup

### Add data and create a backup

put 'ns1:t1', 'key6', 'cf1:c1', 'value-6'
put 'ns1:t1', 'key7', 'cf1:c1', 'value-7'

hbase backup1 -Dmapreduce.framework.name=local create  -tables "ns1:t1" \
      -backup_root_path file:///tmp/hbase_snapshot_backup

hbase backup1 history \
      -backup_root_path file:///tmp/hbase_snapshot_backup

### Show the backup history
hbase backup1 history \
      -backup_root_path file:///tmp/hbase_snapshot_backup

### Restore a backup
# From the history, select a backup to restore

sudo -u hbase hbase backup1 -Dmapreduce.framework.name=local restore  \
         -backup_root_path file:///tmp/hbase_snapshot_backup \
         -backup_id its_backup_1552371238310


### Restore to another cluster
# Restore a whole backup
sudo -u hbase hbase backup1 -Dmapreduce.framework.name=local restore  \
-backup_root_path file:///tmp/hbase_snapshot_backup  \
-restore_root_path  hdfs://sandbox-hdp.hortonworks.com:8020/apps/hbase/data \
-backup_id its_backup_1549523726949
# Partial Restore
# Restore ...


### You can set the number of backups to retains

hbase backup1 history  -backup_root_path file:///tmp/hbase_snapshot_backup
hbase backup1 rollup -backup_root_path file:///tmp/hbase_snapshot_backup -nb_backups 2

Or while creating the backups:

hbase backup1 create  -namespaces "ns1" \
      -backup_root_path file:///tmp/hbase_snapshot_backup -rollup 2


### Dynamique rajouter des tables ds le namespace




sudo -u hbase hbase backup1 -Dmapreduce.framework.name=local restore    -backup_id its_backup_1551021248638   -backup_root_path file:///tmp/hbase_snapshot_backup  -mapping ns1:t1=nsT:ta -mapping ns1:t2=nsA:tb

