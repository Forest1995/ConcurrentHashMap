package Testcases;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

public class ThreadTestGet extends ThreadsTestRunableBase {
    public ThreadTestGet(int threadId, MyConcurrentHashTable<Integer, String> table, ArrayList<Integer> workload) {
        super(threadId, table, workload);
    }
    public ThreadTestGet(){
        super();
    }
    @Override
    public void run() {
        for (Integer integer : workload) {
            testGet(integer);
        }

    }
}
