
### Create the tables
create_namespace 'ns2'
create 'ns2:t1', 'cf1'
create 'ns2:t2', 'cf1'
create 'ns2:t3', 'cf1'

### Put data

put 'ns2:t1', 'key1', 'cf1:c1', 'value-1'
put 'ns2:t1', 'key2', 'cf1:c1', 'value-2'
put 'ns2:t1', 'key3', 'cf1:c1', 'value-3'

put 'ns2:t2', 'key1', 'cf1:c1', 'value-1'
put 'ns2:t2', 'key2', 'cf1:c1', 'value-2'
put 'ns2:t2', 'key3', 'cf1:c1', 'value-3'

put 'ns2:t3', 'key1', 'cf1:c1', 'value-1'
put 'ns2:t3', 'key2', 'cf1:c1', 'value-2'

### Create a backup set for tables ns2:t1 and ns2:t2
hbase backup2 set add ns2Set "ns2:t1,ns2:t2"
hbase backup2 set list

### Create the full backup for the backup set
# The first backup should be a full backup

hbase backup2 -Dmapreduce.framework.name=local \
      create full file:///tmp/backup/hbase-backups  \
      -s ns2Set

# You can also set the individual tables to backup
hbase backup2 -Dmapreduce.framework.name=local  create full file:///tmp/backup/hbase-backups \
              -t "ns2:t1,ns2:t2"

# You can backup the whole hbase
hbase backup2 -Dmapreduce.framework.name=local \
      create full file:///tmp/backup/hbase-backups  \
      -s ns2Set

### Insert some data on table ns2:t1 and make an incremental backup

java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t1  inc01 2

hbase backup2 -Dmapreduce.framework.name=local \
      create incremental file:///tmp/backup/hbase-backups  \
      -s ns1Set

### Insert some data on table ns2:t2 and make an incremental backup

java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t1  inc02 2

hbase backup2 -Dmapreduce.framework.name=local \
      create incremental file:///tmp/backup/hbase-backups  \
      -s ns1Set

### Insert some data on table ns2:t1, ns2:t2 and make an incremental backup
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t1  inc03 3
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t2  inc03 3


hbase backup2 -Dmapreduce.framework.name=local \
      create incremental file:///tmp/backup/hbase-backups  \
      -s ns1Set

#### History
hbase backup2 history

### Restore an incremental backup (inc01)
# The table ns2:t1  should contains only rows from full0 and inc01
# The table ns2:t2 should contain only rows from full0

hbase restore2 -Dmapreduce.framework.name=local -s ns1Set -o file:///tmp/backup/hbase-backups backup_1549452783401


### Restore full backup (full0)

hbase restore2 -Dmapreduce.framework.name=local -s ns1Set -o file:///tmp/backup/hbase-backups backup_1549452729010

### Drop the table ns2:t1 and restore it from inc03
disable 'ns2:t1'
drop 'ns2:t1'

hbase restore2 -Dmapreduce.framework.name=local -t "ns2:t1" -o file:///tmp/backup/hbase-backups backup_1549452992943

### Merge the backups inc01, inc02, inc3
# You need to merge the laste incremental backups before running an incremental bk, otherwise u will not able to restore from that incremental bk (the merged backup id is removed only from the merged  image and ancestors, still refererenced by the latests images)
hbase backup2 -Dmapreduce.framework.name=local merge  "backup_1549452783401,backup_1549452992943, backup_1549452966868"

hbase backup2 history

### Create an incremental bk
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t2  inc04 3
hbase backup2 -Dmapreduce.framework.name=local \
      create incremental file:///tmp/backup/hbase-backups  \
      -s ns1Set

### Restore the last incremental
java -jar /tmp/hbase-data-1.0-SNAPSHOT.jar  ns1  t2  inc05 3
hbase restore2 -Dmapreduce.framework.name=local -s ns1Set -o file:///tmp/backup/hbase-backups backup_1549515889703


### Add other tables to the backup set and perform an incremental backup
hbase backup2 set add ns1Set "ns2:t3"
hbase backup2 set list
hbase backup2 -Dmapreduce.framework.name=local \
      create incremental file:///tmp/backup/hbase-backups  \
      -s ns1Set

java.io.IOException: Some tables (ns2:t3) haven't gone through full backup. Perform full backup on ns2:t3 first, then retry the command

### Drop the Column Family: the table could bot be restored, only the column family
alter 'ns2:t1',{ NAME=>'cf2',METHOD=>'delete' }


alter 'ns2:t1',{ NAME=>'cf2' }

### Restore the CF from merged backup

### You cannot merge the full backup and the uncremental ones

hbase backup2 -Dmapreduce.framework.name=local  merge "backup_1549452729010,backup_1549452966868"

java.io.IOException: FULL backup image can not be merged for:

### Delete a backup
hbase backup2 delete backup_1549516790773

### Restore on another cluster
# => Currently, the tables backup:system and backup:system_bulk are not bakuped
# We use the hbase org.apache.hadoop.hbase.mapreduce.Export utilty to backup them on the filesystem

hbase -Dmapreduce.framework.name=local org.apache.hadoop.hbase.mapreduce.Export -Dmapreduce.framework.name=local backup:system  file:///tmp/backup/hbase-backup_system/backup_system

hbase -Dmapreduce.framework.name=local org.apache.hadoop.hbase.mapreduce.Export -Dmapreduce.framework.name=local backup:system_bulk  file:///tmp/backup/hbase-backup_system/backup_system_bulk

disable_all 'backup:system.*'
drop_all 'backup:system.*'
disable_all 'ns2:.*'
drop_all 'ns2:.*'

# Restore the backup system tables
# The backup should be enabled on the other cluster

hbase org.apache.hadoop.hbase.mapreduce.Import -Dmapreduce.framework.name=local backup:system  file:///tmp/backup/hbase-backup_system/backup_system

hbase org.apache.hadoop.hbase.mapreduce.Import -Dmapreduce.framework.name=local backup:system_bulk  file:///tmp/backup/hbase-backup_system/backup_system_bulk

# Start the restore
hbase restore2 -Dmapreduce.framework.name=local -s ns1Set -o file:///tmp/backup/hbase-backups backup_1549452729010

# Pas dynamique en cas d'ajout de tables ds le namespace, backup separé
# Si le backup fail, faire un repair
# Backuper les tables de backups



Troubleshooting:

This may indicate that a previous session has failed abnormally.
In this case, backup recovery is recommended.
2019-03-12 06:25:57,750 ERROR [main] hbase1_2_1.BackupDriver: Error running command-line tool
java.io.IOException: Active session found, aborted command execution
	at org.apache.hadoop.hbase.backup.hbase1_2_1.impl.BackupCommands$Command.execute(BackupCommands.java:139)


 hbase backup2 repair