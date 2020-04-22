import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;

import MyConcurrentHashTable.*;
import ThreadsTest.ThreadsTest1;

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
    public static void poolTest() throws InterruptedException {
        MyConcurrentHashTable<Integer,String> map=new ParallelHashMapWIthChains<Integer,String>();
        int numThread=3;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThread);


        for(int i=0;i<numThread;i++){
            Runnable runable=new ThreadsTest1(i,map);
            executor.execute(runable);
        }

        executor.shutdown();
        while (!executor.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }
        System.out.println(map.toString());;
    }
}
