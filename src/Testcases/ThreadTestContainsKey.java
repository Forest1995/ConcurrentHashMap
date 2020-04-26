package Testcases;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

public class ThreadTestContainsKey extends ThreadsTestRunableBase {
    public ThreadTestContainsKey(int threadId, MyConcurrentHashTable<Integer, String> table, ArrayList<Integer> workload) {
        super(threadId, table, workload);
    }
    public ThreadTestContainsKey(){
        super();
    }
    @Override
    public void run() {
        for (Integer integer : workload) {
            testContainsKey(integer);
        }

    }
}
