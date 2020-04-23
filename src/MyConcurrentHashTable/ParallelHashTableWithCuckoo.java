package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithCuckoo<K, V> implements MyConcurrentHashTable<K, V> {

    private int slotSize = 8;
    private int size = 0;
    private HashTableEntry<K, V>[] slots;
    private ReentrantLock[] locks;
    private ReentrantLock sizeLock;
    private List<HashAlgorithm> hashAlgorithmList = new ArrayList<HashAlgorithm>();


    public ParallelHashTableWithCuckoo() {
        hashAlgorithmList.add(new HashAlgorithm(10));
        hashAlgorithmList.add(new HashAlgorithm(23));
        slots = new HashTableEntry[slotSize];
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
            int slotIdx = hashCode & (slotSize - 1);
            locks[slotIdx].lock();
            try {
                if (slots[slotIdx] != null && slots[slotIdx].getKey().equals(key))
                    return slots[slotIdx].getValue();
            } finally {
                locks[slotIdx].unlock();
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(K key) {
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode & (slotSize - 1);
            locks[slotIdx].lock();
            try {
                if (slots[slotIdx] != null && slots[slotIdx].getKey().equals(key))
                    return true;
            } finally {
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
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode & (slotSize - 1);
            locks[slotIdx].lock();
            try {
                if (slots[slotIdx] != null && slots[slotIdx].getKey().equals(key)) {
                    V removedValue = slots[slotIdx].getValue();
                    slots[slotIdx] = null;
                    return removedValue;
                }
            } finally {
                locks[slotIdx].unlock();
            }
        }
        return null;
    }

    @Override
    public void clear() {
        for(int i=0;i<slotSize;i++){
            locks[i].lock();
        }
        for(int i=0;i<slotSize;i++){
            slots[i] = null;
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
        return 0;
    }
}
