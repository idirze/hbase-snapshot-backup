package com.idirze.hbase.snapshot.backup.commad.impl;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
public class HbaseBackupRestoreTables {

    private List<HbaseBackupRestoreTable> tasks = new ArrayList<>();

    private ExecutorService executor;

    public HbaseBackupRestoreTables(int nbTasks) {
        executor = Executors.newFixedThreadPool(nbTasks);
    }

    public HbaseBackupRestoreTables add(HbaseBackupRestoreTable task) {
        tasks.add(task);
        return this;
    }


    public int execute() {
        int res = 0;
        for (HbaseBackupRestoreTable task : tasks) {
            if (task.execute() == -1) {
                res = -1;
            }
        }

        return res;
    }

    public int submit() {
        int res = 0;
        List<Future<Integer>> futures = new ArrayList<>();
        tasks
                .stream()
                .forEach(t -> futures.add(executor.submit(() -> t.execute())));

        for (Future<Integer> f : futures) {
            try {
                if (f.get() == -1) {
                    res = -1;
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("", e);
            }

        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }
        }

        return res;

    }

}
