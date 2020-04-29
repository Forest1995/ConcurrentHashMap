package Testcases;

import MyConcurrentHashTable.*;

import static org.junit.jupiter.api.Assertions.*;

class MethodTestSingleThread {
    private int size=1000000;
    MyConcurrentHashTable<Integer,String> initialize(int size){
        MyConcurrentHashTable<Integer,String> table= new ParallelHashTableWithHopscotch<Integer, String>();
        for(int i=0;i<size;i++){
            table.put(i,""+i);
        }
        return table;
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
        int size=0;
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
    void put() {
//        int size=1000000;
        MyConcurrentHashTable<Integer,String>  table=initialize(size);
        for(int i=0;i<size;i++){
            assertTrue(table.containsKey(i));
        }
        assertEquals(table.size(),size);
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