package MyConcurrentHashTable;

import com.sun.tools.classfile.ConstantPool;
import com.sun.tools.javac.util.Assert;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashMapWIthChains<K,V> implements MyConcurrentHashTable<K,V> {
    private int slotSize=8;
    private int size=0;
    private ArrayList<LinkedList<HashTableEntry<K,V>>> slots;
    private ReentrantLock[] locks;
    private ReentrantLock sizeLock;
//    private ReentrantLock
    public ParallelHashMapWIthChains(){
        slots=new ArrayList<>(slotSize);
        locks=new ReentrantLock[slotSize];
        sizeLock=new ReentrantLock();
        for(int i=0;i<slotSize;i++){
            slots.add(new LinkedList<HashTableEntry<K, V>>());
            locks[i]=new ReentrantLock();
        }
    }
    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size==0;
    }

    @Override
    public boolean containsKey(K key) {
        int hash=hash(key);
        int slotIdx=hash&(slotSize-1);
//        System.out.println("[ContainsKey]:"+slotIdx);
        locks[slotIdx].lock();
        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);
            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
//                System.out.println("[ContrainsKeyCmp]"+entry.getKey().equals(key));
                if (entry.getKey().equals(key)) {
                    //exists
//                    System.out.println("return true");
                    return true;
                }
            }
            //not found
//            System.out.println("return false");
            return false;
        }catch (Exception e){
            System.err.println("[containsKey err]"+e.getMessage());
            return false;
        }
        finally {
            locks[slotIdx].unlock();

        }
    }


    @Override
    public V get(K key) {
        int slotIdx=hash(key)&(slotSize-1);
        locks[slotIdx].lock();
        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);
            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
                if (entry.getKey().equals(key)) {
                    // exists
                    return entry.getValue();
                }
            }
            //not found
            return null;
        }finally {
            locks[slotIdx].unlock();
        }
    }

    @Override
    public void put(K key, V value)
    {
        putVal(hash(key),key,value);
    }
    private void putVal(int hash, K key, V value){
        int slotIdx=hash&(slotSize-1);
        locks[slotIdx].lock();
        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);
            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
                if (entry.getKey().equals(key)) {
                    //already exists
//                    V temp = entry.getValue();
                    entry.setValue(value);
//                    return temp;
                }
            }

            slot.add(new HashTableEntry<K, V>(key, value));
            slots.set(slotIdx, slot);
            sizeLock.lock();
            size++;
            sizeLock.unlock();
//            return null;
        }catch (Exception e){
            System.err.println("[putval err]"+e.getMessage());
//            return null;
        }
        finally {
            locks[slotIdx].unlock();
        }


    }
    @Override
    public V remove(K key) {
        int slotIdx=hash(key)&(slotSize-1);
        locks[slotIdx].lock();
        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);

            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
                if (entry.getKey().equals(key)) {
                    //already exists
                    V temp = entry.getValue();
                    slot.remove(entry);
                    sizeLock.lock();
                    size--;
                    sizeLock.unlock();
                    return temp;
                }
            }

            return null;
        }finally {
            locks[slotIdx].unlock();
        }

    }


    @Override
    public void clear() {
        for(int i=0;i<slotSize;i++){
            locks[i].lock();
        }
        for(int i=0;i<slotSize;i++){
            slots.set(i,new LinkedList<HashTableEntry<K, V>>());
        }
        sizeLock.lock();
        size=0;
        sizeLock.unlock();
        for(int i=slotSize-1;i>=0;i--){
            locks[i].unlock();
        }
    }

    @Override
    public int hash(K key) {
        return key.hashCode();
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb=new StringBuilder();
        int slotIdx=0;
        for(LinkedList<HashTableEntry<K,V>> slot:slots){
            sb.append("slot").append(slotIdx++).append(": ");
            if(slot!=null){
                for(HashTableEntry<K,V> entry: slot){
                    sb.append("key=");
                    sb.append(entry.getKey().toString());
                    sb.append(" val=");
                    sb.append(entry.getValue().toString());
                    sb.append('|');
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
