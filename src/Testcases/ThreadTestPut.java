package Testcases;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThreadTestPut extends ThreadsTestRunableBase {
    public ThreadTestPut(int threadId, MyConcurrentHashTable<Integer, String> table, ArrayList<Integer> workload) {
        super(threadId, table, workload);
    }
    public ThreadTestPut(){
        super();
    }
    @Override
    public void run() {
        for(int i = 0; i<workload.size(); i++){
            testPut(workload.get(i));
        }

    }
}
