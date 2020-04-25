package Testcases;

import MyConcurrentHashTable.*;
import ThreadsTest.ThreadsTest2;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MethodTestMultiThread {
    private int size=1000000;
    private int numThread=4;
    private int total_workload=1000000;
    ArrayList<ArrayList<Integer>> workloads;
    int workPerThread;
    MyConcurrentHashTable<Integer,String> initialize(int size){
        MyConcurrentHashTable<Integer,String> table= new ParallelHashMapWIthChains<Integer, String>();//modify table type here.
        for(int i=0;i<size;i++){
            table.put(i,""+i);
        }
        return table;
    }
    ArrayList<ArrayList<Integer>> getWorkLoads(){
        if(workloads==null){
            return generateDifferentInt(numThread,workPerThread);
        }else{
            return workloads;
        }
    }
    static ArrayList<ArrayList<Integer>> generateDifferentInt(int numThread, int size){
        ArrayList<ArrayList<Integer>> result=new ArrayList<>();
        Random random=new Random();
//        MyConcurrentHashTable<Integer,Integer> map=new ParallelHashMapWIthChains<Integer, Integer>();
        HashSet<Integer> set=new HashSet<>();
        for(int k=0;k<numThread;k++){
            result.add(new ArrayList<>());
            for(int i=0;i<size;i++){
                int num=random.nextInt();
                while(set.contains(num)){
                    num=random.nextInt();
                }
                result.get(k).add(num);
                set.add(num);
            }
        }
        return result;
    }
    static ArrayList<ArrayList<Integer>> generateInt(int numThread, int size){
        ArrayList<ArrayList<Integer>> result=new ArrayList<>();
        Random random=new Random();
        for(int k=0;k<numThread;k++){
            result.add(new ArrayList<>());
            for(int i=0;i<size;i++){
                int num=random.nextInt();
                result.get(k).add(num);
            }
        }
        return result;
    }

    public void poolTest(MyConcurrentHashTable<Integer,String> map,ThreadsTestRunableBase runnable) throws InterruptedException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
//        MyConcurrentHashTable<Integer,String> map=new ParallelHashMapWIthChainsLockFree<Integer, String>();
//        MyConcurrentHashTable<Integer,String> map=new ParallelHashTableWithCuckoo<Integer, String>();

        workPerThread=total_workload/numThread;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);


        //prepare data

        workloads=generateDifferentInt(numThread,workPerThread);
//        ArrayList<ArrayList<Integer>> workloads= generateInt(numThread,workPerThread);


        //start inserting data into map
        long startTime = System.nanoTime();
        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase curRunnable=runnable.getClass().getDeclaredConstructor().newInstance();
            curRunnable.setTable(map);
            curRunnable.setThreadId(i);
            curRunnable.setWorkload(workloads.get(i));

            executor.execute(curRunnable);
        }
        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }


        long estimatedTime = System.nanoTime() - startTime;
//        System.out.println(map.toString());;
//        System.out.println(map.size());
        System.out.println(estimatedTime/1000/1000+"ms");
    }
    @org.junit.jupiter.api.Test
    void size() {
//        int size=1000000;

        MyConcurrentHashTable<Integer,String>  table=initialize(size);
        assertEquals(table.size(), size);
    }

    @org.junit.jupiter.api.Test
    void get() {
//        int size=1000000;
        MyConcurrentHashTable<Integer,String>  table=initialize(size);
        for(int i=0;i<size;i++){
            String value=table.get(i);
            assertEquals(value, "" + i);
        }
    }

    @org.junit.jupiter.api.Test
    void isEmpty() {
//        int size=0;
        MyConcurrentHashTable<Integer,String>  table=initialize(size);
        assertTrue(table.isEmpty());
        table.put(1,"1");
        assertFalse(table.isEmpty());
        table.remove(1);
        assertTrue(table.isEmpty());
    }

    @org.junit.jupiter.api.Test
    void containsKey() {
//        int size=1000000;
        MyConcurrentHashTable<Integer,String>  table=initialize(size);
        for(int i=0;i<size;i++){
            assertTrue(table.containsKey(i));
        }

    }

    @org.junit.jupiter.api.Test
    void put() throws InterruptedException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
//        int size=1000000;
        MyConcurrentHashTable<Integer,String>  table=initialize(0);
        poolTest(table,new ThreadTestPut(-1,null,null));
        for(ArrayList<Integer> workload :workloads){
            for(Integer i : workload){
                assertTrue(table.containsKey(i));
            }
        }
        assertEquals(total_workload,table.size());
    }

    @org.junit.jupiter.api.Test
    void remove() {
//        int size=1000000;
        MyConcurrentHashTable<Integer,String>  table=initialize(size);
        for(int i=0;i<size;i++){
            assertTrue(table.containsKey(i));
        }
        for(int i=0;i<size;i++){
            table.remove(i);
        }
        for(int i=0;i<size;i++){
            assertFalse(table.containsKey(i));
        }
        assertEquals(table.size(),0);
    }

    @org.junit.jupiter.api.Test
    void clear() {
//        int size=1000000;
        MyConcurrentHashTable<Integer,String>  table=initialize(size);
        for(int i=0;i<size;i++){
            assertTrue(table.containsKey(i));
        }
        table.clear();
        for(int i=0;i<size;i++){
            assertFalse(table.containsKey(i));
        }
        assertEquals(table.size(),0);

    }

}