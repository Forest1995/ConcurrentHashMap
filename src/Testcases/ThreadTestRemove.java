package Testcases;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

public class ThreadTestRemove extends ThreadsTestRunableBase {
    public ThreadTestRemove(int threadId, MyConcurrentHashTable<Integer, String> table, ArrayList<Integer> workload) {
        super(threadId, table, workload);
    }
    public ThreadTestRemove(){
        super();
    }
    @Override
    public void run() {
        for (Integer integer : workload) {
            testRemove(integer);
        }

    }
}
