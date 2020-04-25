package MyConcurrentHashTable;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithHopscotch<K, V> implements MyConcurrentHashTable<K, V> {
    private volatile AtomicInteger size;
    private volatile AtomicInteger slotSize;
    private volatile HashTableEntry<K, V>[] slots;
    private volatile ReentrantLock[] locks;

    public ParallelHashTableWithHopscotch() {
        size = new AtomicInteger(0);
        slotSize = new AtomicInteger(8);
        slots = new HashTableEntry[slotSize.get()];
        locks = new ReentrantLock[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public void put(K key, V value) {

    }

    @Override
    public V remove(K key) {
        return null;
    }

    @Override
    public void clear() {
        for (int i = 0; i < slotSize.get(); i++) {
            locks[i].lock();
        }
        for (int i = 0; i < slotSize.get(); i++) {
            slots[i] = null;
        }
        size.set(0);
        for (int i = slotSize.get() - 1; i >= 0; i--) {
            locks[i].unlock();
        }
    }

    @Override
    public int hash(K key) {
        int hash = key.hashCode();
        hash %= slotSize.get();
        if (hash < 0) {
            hash += slotSize.get();
        }
        return hash;
    }
}
