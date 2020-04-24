package ThreadsTest;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

public class ThreadsTest2 implements Runnable {
    private MyConcurrentHashTable<Integer,String> table;
    public Integer threadId;
    public ArrayList<Integer> workload;
    public ThreadsTest2(int threadId, MyConcurrentHashTable<Integer,String> table, ArrayList<Integer> workload){
        this.table=table;
        this.threadId=threadId;
        this.workload=workload;
    }
    @Override
    public void run() {
//        for(int i = 0; i<workload; i++){
//
//            testPutAndContainsKey(i);
//
//        }
//        for(int i = 0; i<workload.size(); i++){
//
//            testGet(workload.get(i));
//
//        }
//        try {
//            Thread.sleep(1000);
//            testClear();
//            Thread.sleep(1000);
//
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        System.out.println("finish phase1");
        try {
            for(int i=0;i<workload.size();i++){

                testPutAndContainsKey(workload.get(i));
//                testContrainsNextKey(workload.get(i));
                testGet(workload.get(i));
                testRemove(workload.get(i));
            }
        }catch (Exception e){
            System.err.println(e.getMessage());
        }


    }
    private void testPutAndContainsKey(int index){
//        int key=threadId*128+index;

        int key=index;
        table.put(key,threadId.toString());
        boolean query=table.containsKey(key);
//        System.out.println(query);
        assert query :"not found in "+threadId+ " for searching "+key;

    }
    private void testContrainsNextKey(int index){
//        int key=threadId*128+index;
        int key=index;
        boolean query=table.containsKey(key+1);
        assert !query:"should not be found!";
    }
    private void testGet(int index){
//        int key=threadId*128+index;
        int key=index;
        String query=table.get(key);
        assert query!=null :"not found "+key +" in " +threadId;

//        query=table.get(key+1);
//        assert query==null:"should not be found!";
    }
    private void testRemove(int index){
//        int key=threadId*128+index;
        int key=index;
        String query=table.remove(key);
        assert query!=null :"remove not found："+key;
//        if(query==null){
//            System.out.println("error");
//        }

//        query=table.remove(key+1);
//        assert query==null:"should not be found!";
    }
    private void testClear(){
        table.clear();
    }
}
