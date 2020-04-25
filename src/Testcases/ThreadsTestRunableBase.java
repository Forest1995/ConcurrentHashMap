package Testcases;

import MyConcurrentHashTable.MyConcurrentHashTable;

import java.util.ArrayList;

public class ThreadsTestRunableBase implements Runnable {
    public MyConcurrentHashTable<Integer,String> table;
    public Integer threadId;
    public ArrayList<Integer> workload;
    public ThreadsTestRunableBase(int threadId, MyConcurrentHashTable<Integer,String> table, ArrayList<Integer> workload){
        this.table=table;
        this.threadId=threadId;
        this.workload=workload;
    }
    ThreadsTestRunableBase(){
        table=null;
        threadId=null;
        workload=null;
    }

    public void setTable(MyConcurrentHashTable<Integer, String> table) {
        this.table = table;
    }

    public void setThreadId(Integer threadId) {
        this.threadId = threadId;
    }

    public void setWorkload(ArrayList<Integer> workload) {
        this.workload = workload;
    }

    @Override
    public void run() {
        for(int i = 0; i<workload.size(); i++){

            testPutAndContainsKey(workload.get(i));

        }

    }
    void testPut(int index){
//        int key=threadId*128+index;

        int key=index;
        table.put(key,threadId.toString());

    }
    void testPutAndContainsKey(int index){
//        int key=threadId*128+index;

        int key=index;
        table.put(key,threadId.toString());
        boolean query=table.containsKey(key);
//        System.out.println(query);
        assert query :"not found in "+threadId+ " for searching "+key;

    }
    void testContrainsNextKey(int index){
//        int key=threadId*128+index;
        int key=index;
        boolean query=table.containsKey(key+1);
        assert !query:"should not be found!";
    }
    void testGet(int index){
//        int key=threadId*128+index;
        int key=index;

        String query=table.get(key);
        assert query!=null :"not found "+key +" in " +threadId;

        query=table.get(key+1);
        assert query==null:"should not be found!";
    }
    void testRemove(int index){
        int key=threadId*128+index;

        String query=table.remove(key);
        assert query!=null :"remove not found";

        query=table.remove(key+1);
        assert query==null:"should not be found!";
    }
    void testClear(){
        table.clear();
    }
}
