package Testcases;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

public class ThreadTestSize extends ThreadsTestRunableBase {
    public ThreadTestSize(int threadId, MyConcurrentHashTable<Integer, String> table, ArrayList<Integer> workload) {
        super(threadId, table, workload);
    }
    public ThreadTestSize(){
        super();
    }
    @Override
    public void run() {
        for (Integer integer : workload) {
            testSize();
        }

    }
}
