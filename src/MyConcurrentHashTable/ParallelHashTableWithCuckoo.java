package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithCuckoo<K, V> implements MyConcurrentHashTable<K, V> {

    private int slotSize = 8;
    private int size = 0;
    private ArrayList<HashTableEntry<K, V>> slots;
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
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode & (slotSize-1);
            locks[slotIdx].lock();
            try{
                if(slots.get(slotIdx)!=null && slots.get(slotIdx).getKey().equals(key))
                    return slots.get(slotIdx).getValue();
            }
            finally {
                locks[slotIdx].unlock();
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return size==0;
    }

    @Override
    public boolean containsKey(K key) {
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode & (slotSize-1);
            locks[slotIdx].lock();
            try{
                if(slots.get(slotIdx)!=null&& slots.get(slotIdx).getKey().equals(key))
                    return true;
            }
            finally {
                locks[slotIdx].unlock();
            }
        }
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
