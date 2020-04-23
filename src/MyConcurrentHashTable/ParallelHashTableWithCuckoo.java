package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithCuckoo<K, V> implements MyConcurrentHashTable<K, V> {

    private int slotSize = 8;
    private int size = 0;
    private ArrayList<V> slots;
    private ReentrantLock[] locks;
    private List<HashAlgorithm> hashAlgorithmList = new ArrayList<HashAlgorithm>();


    public ParallelHashTableWithCuckoo() {
        hashAlgorithmList.add(new HashAlgorithm(10));
        hashAlgorithmList.add(new HashAlgorithm(23));
        slots = new ArrayList<>(slotSize);
        locks = new ReentrantLock[slotSize];
        for (int i = 0; i < slotSize; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(K key) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public int hash(K key) {
        return 0;
    }
}

class HashAlgorithm <K> {
    private int initNumber;
    public HashAlgorithm(int initNumber) {
        this.initNumber = initNumber;
    }

    public int hashCode(K key) {
        return  key.hashCode()+ initNumber ;
    }
}