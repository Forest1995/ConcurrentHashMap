import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.*;

import MyConcurrentHashTable.*;
import Testcases.*;
import ThreadsTest.ThreadsTest1;
import ThreadsTest.ThreadsTest2;

public class Main {

    public static void main(String[] args) {
        try {
            poolTest();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }

	// write your code here
//        ArrayList<String> arr=new ArrayList<>();
//        arr.add("123");
//        String str1=arr.get(0);
//        str1="234";
//        System.out.println(arr.get(0));

    }
    public static void simpleTest(){
        MyConcurrentHashTable<Integer,String> map=new ParallelHashTableWithCuckoo<Integer, String>();
        for(Integer i=0;i<10;i++){
            map.put(i,i.toString());
        }
        System.out.println(map.toString());
    }

    public static ArrayList<ArrayList<Integer>> generateDifferentInt(int numThread, int size){
        ArrayList<ArrayList<Integer>> result=new ArrayList<>();
        Random random=new Random();
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
    public static void poolTest() throws InterruptedException {
//        MyConcurrentHashTable<Integer,String> map=new ParallelHashTableWithHopscotchLockFree<Integer, String>();
        MyConcurrentHashTable<Integer,String> map=new ParallelHashMapWIthChains<Integer, String>();
        int numThread=1;
        int total_workload=1000000;
        int workPerThread=total_workload/numThread;
        ArrayList<ArrayList<Integer>> workloads=generateDifferentInt(numThread,workPerThread);

        System.out.println("before testing, warmup without cache, without any resize() before");
        testPut(numThread,map,workloads,total_workload); //without cache
        testClear(numThread,map,workloads,total_workload);

        System.out.println("------------start performance test part1 for "+map.getClass().getName()+"------------\n\n");
        //prepare data
        testPut(numThread,map,workloads,total_workload);//with cache
        testContainsKey(numThread,map,workloads,total_workload);
        testSize(numThread,map,workloads,total_workload);
        testRemove(numThread,map,workloads,total_workload);

        System.out.println("------------start performance test part2 for "+map.getClass().getName()+"------------\n\n");
        testIsEmpty(numThread,map,workloads,total_workload);
        testPut(numThread,map,workloads,total_workload);
        testGet(numThread,map,workloads,total_workload);
        testClear(numThread,map,workloads,total_workload);
        //test performance of put


    //prepare job
//
//        System.out.println(map.size());
//        System.out.println(estimatedTime/1000/1000+"ms");
    }
    static void  testPut(int numThread, MyConcurrentHashTable<Integer,String> map, ArrayList<ArrayList<Integer>> workloads, int total_workload) throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        long startTime = System.nanoTime();

        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase runnable=new ThreadTestPut(i,map,workloads.get(i));
            executor.execute(runnable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(estimatedTime/1000/1000+"ms for put() with "+numThread+" threads, TotalWorkload:"+total_workload);
    }
    static void  testContainsKey(int numThread, MyConcurrentHashTable<Integer,String> map, ArrayList<ArrayList<Integer>> workloads, int total_workload) throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        long startTime = System.nanoTime();

        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase runnable=new ThreadTestContainsKey(i,map,workloads.get(i));
            executor.execute(runnable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(estimatedTime/1000/1000+"ms for ContainsKey() with "+numThread+" threads, TotalWorkload:"+total_workload);
    }
    static void  testSize(int numThread, MyConcurrentHashTable<Integer,String> map, ArrayList<ArrayList<Integer>> workloads, int total_workload) throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        long startTime = System.nanoTime();

        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase runnable=new ThreadTestSize(i,map,workloads.get(i));
            executor.execute(runnable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(estimatedTime/1000/1000+"ms for size() with "+numThread+" threads, TotalWorkload:"+total_workload);
    }
    static void  testGet(int numThread, MyConcurrentHashTable<Integer,String> map, ArrayList<ArrayList<Integer>> workloads, int total_workload) throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        long startTime = System.nanoTime();

        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase runnable=new ThreadTestGet(i,map,workloads.get(i));
            executor.execute(runnable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(estimatedTime/1000/1000+"ms for get() with "+numThread+" threads, TotalWorkload:"+total_workload);
    }
    static void  testRemove(int numThread, MyConcurrentHashTable<Integer,String> map, ArrayList<ArrayList<Integer>> workloads, int total_workload) throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        long startTime = System.nanoTime();

        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase runnable=new ThreadTestRemove(i,map,workloads.get(i));
            executor.execute(runnable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(estimatedTime/1000/1000+"ms for remove() with "+numThread+" threads, TotalWorkload:"+total_workload);
    }

    static void  testClear(int numThread, MyConcurrentHashTable<Integer,String> map, ArrayList<ArrayList<Integer>> workloads, int total_workload) throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        long startTime = System.nanoTime();

        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase runnable=new ThreadTestClear(i,map,workloads.get(i));
            executor.execute(runnable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(estimatedTime/1000/1000+"ms for clear() with "+numThread+" threads, TotalWorkload:"+total_workload);
    }
    static void  testIsEmpty(int numThread, MyConcurrentHashTable<Integer,String> map, ArrayList<ArrayList<Integer>> workloads, int total_workload) throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        long startTime = System.nanoTime();

        for(int i=0;i<numThread;i++){
            ThreadsTestRunableBase runnable=new ThreadTestIsEmpty(i,map,workloads.get(i));
            executor.execute(runnable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println(estimatedTime/1000/1000+"ms for isEmpty() with "+numThread+" threads, TotalWorkload:"+total_workload);
    }

}
