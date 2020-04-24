import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.*;

import MyConcurrentHashTable.*;
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
        ParallelHashMapWIthChains<Integer,String> map=new ParallelHashMapWIthChains<Integer, String>();
        for(Integer i=0;i<10;i++){
            map.put(i,i.toString());
        }
        System.out.println(map.toString());
    }
    public static ArrayList<ArrayList<Integer>> generateDifferentInt(int numThread, int size){
        ArrayList<ArrayList<Integer>> result=new ArrayList<>();
        Random random=new Random();
        MyConcurrentHashTable<Integer,Integer> map=new ParallelHashMapWIthChains<Integer, Integer>();
//        HashSet<Integer> set=new HashSet<>();
        for(int k=0;k<numThread;k++){
            result.add(new ArrayList<>());
            for(int i=0;i<size;i++){
                int num=random.nextInt();
                while(map.containsKey(num)){
                    num=random.nextInt();
                }
                result.get(k).add(num);
                map.put(num,num);
            }
        }
        return result;
    }
    public static void poolTest() throws InterruptedException {
//        MyConcurrentHashTable<Integer,String> map=new ParallelHashMapWIthChains<Integer,String>();
        MyConcurrentHashTable<Integer,String> map=new ParallelHashMapWIthChainsLockFree<Integer, String>();
        int numThread=4;
        int total_workload=1000000;
        int workPerThread=total_workload/numThread;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);


    //prepare data

        Random random=new Random();
        ArrayList<ArrayList<Integer>> workloads=generateDifferentInt(numThread,workPerThread);
//        ArrayList<ArrayList<Integer>> workloads=new ArrayList<ArrayList<Integer>>();
//        for(int i=0;i<numThread;i++){
//            ArrayList<Integer> workload=new ArrayList<Integer>();
//            for(int j=0;j<workPerThread;j++){
//                workload.add(random.nextInt());
//            }
//            workloads.add(workload);
//
//        }

        //start inserting data into map


        for(int i=0;i<numThread;i++){
            Runnable runable=new ThreadsTest1(i,map,workloads.get(i));
            executor.execute(runable);
        }
        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }

    //prepare job
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);
        Runnable[] jobList=new Runnable[numThread];
        //start running
        long startTime = System.nanoTime();
        System.out.println("data ready, start running");

        for(int i=0;i<numThread;i++){
            jobList[i]=new ThreadsTest2(i,map,workloads.get(i));

        }
        for(int i=0;i<numThread;i++){
            executor.execute(jobList[i]);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }

        long estimatedTime = System.nanoTime() - startTime;
//        System.out.println(map.toString());;
        System.out.println(map.size());
        System.out.println(estimatedTime/1000/1000+"ms");
    }
}
