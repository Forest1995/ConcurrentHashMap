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
            testPutAndContainsKey(i);
            testContrainsNextKey(i);
            testGet(i);
            testRemove(i);

        }

    }
    private void testPutAndContainsKey(int index){
        int key=threadId*128+index;

        table.put(key,threadId.toString());
        boolean query=table.containsKey(key);
        assert query :"not found";

    }
    private void testContrainsNextKey(int index){
        int key=threadId*128+index;

        boolean query=table.containsKey(key+1);
        assert !query:"should not be found!";
    }
    private void testGet(int index){
        int key=threadId*128+index;

        String query=table.get(key);
        assert query==null :"not found";

        query=table.get(key+1);
        assert query!=null:"should not be found!";
    }
    private void testRemove(int index){
        int key=threadId*128+index;

        String query=table.remove(key);
        assert query!=null :"remove not found";

        query=table.remove(key+1);
        assert query==null:"should not be found!";
    }
}
