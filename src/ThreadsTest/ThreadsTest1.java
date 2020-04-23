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



        }
        try {
            Thread.sleep(1000);
            testClear();
            Thread.sleep(1000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("finish phase1");
        try {
            for(int i=0;i<10;i++){

                testPutAndContainsKey(i);
                testContrainsNextKey(i);
                testGet(i);
//                testRemove(i);
            }
        }catch (Exception e){
            System.err.println(e.getMessage());
        }


    }
    private void testPutAndContainsKey(int index){
        int key=threadId*128+index;

        table.put(key,threadId.toString());
        boolean query=table.containsKey(key);
//        System.out.println(query);
        assert query :"not found in "+threadId+ " for searching "+key;

    }
    private void testContrainsNextKey(int index){
        int key=threadId*128+index;

        boolean query=table.containsKey(key+1);
        assert !query:"should not be found!";
    }
    private void testGet(int index){
        int key=threadId*128+index;

        String query=table.get(key);
        assert query!=null :"not found "+key +" in " +threadId;

        query=table.get(key+1);
        assert query==null:"should not be found!";
    }
    private void testRemove(int index){
        int key=threadId*128+index;

        String query=table.remove(key);
        assert query!=null :"remove not found";
//        if(query==null){
//            System.out.println("error");
//        }

        query=table.remove(key+1);
        assert query==null:"should not be found!";
    }
    private void testClear(){
        table.clear();
    }
}
