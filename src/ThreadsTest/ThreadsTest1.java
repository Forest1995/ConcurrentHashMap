package ThreadsTest;

import MyConcurrentHashTable.MyConcurrentHashTable;

public class ThreadsTest1 implements Runnable {
    private MyConcurrentHashTable<Integer,String> table;
    public Integer threadId;
    public ThreadsTest1(int threadId, MyConcurrentHashTable<Integer,String> table){
        this.table=table;
        this.threadId=threadId;
    }
    @Override
    public void run() {
        for(Integer i=0;i<10;i++){

            table.put(threadId*128+i,threadId.toString());
        }

    }
}
