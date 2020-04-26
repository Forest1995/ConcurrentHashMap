package Testcases;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

public class ThreadTestIsEmpty extends ThreadsTestRunableBase {
    public ThreadTestIsEmpty(int threadId, MyConcurrentHashTable<Integer, String> table, ArrayList<Integer> workload) {
        super(threadId, table, workload);
    }
    public ThreadTestIsEmpty(){
        super();
    }
    @Override
    public void run() {
        for (Integer integer : workload) {
            testIsEmpty(true);
        }

    }
}
