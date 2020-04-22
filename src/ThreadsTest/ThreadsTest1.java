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
        for(int i = 0; i<10; i++){
            int key=threadId*128+i;
            table.put(key,threadId.toString());
            boolean query=table.containsKey(key);
            assert query :"not found";
            query=table.containsKey(key+1);
            assert !query:"should not be found!";
        }

    }
}
