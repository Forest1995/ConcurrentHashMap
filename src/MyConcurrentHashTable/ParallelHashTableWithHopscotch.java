package MyConcurrentHashTable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithHopscotch<K, V> implements MyConcurrentHashTable<K, V> {
    private volatile AtomicInteger size;
    private volatile AtomicInteger slotSize;
    private volatile HashTableEntry<K, V>[] slots;
    private volatile ReentrantLock[] locks;
    private volatile AtomicInteger[] distArray;
    private static final int MAX_DIST = 30;
    private static final double MAX_LOAD = 0.1;

    public ParallelHashTableWithHopscotch() {
        size = new AtomicInteger(0);
        slotSize = new AtomicInteger(10);
        slots = new HashTableEntry[slotSize.get()];
        locks = new ReentrantLock[slotSize.get()];
        distArray = new AtomicInteger[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            locks[i] = new ReentrantLock();
            distArray[i] = new AtomicInteger(0);
        }
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public V get(K key) {
        int hash = hash(key);
        for (int i = 0; i < MAX_DIST; i++) {
            int dist = distArray[hash].get();
            if ((dist >> i) % 2 == 1) {
                locks[hash + MAX_DIST - 1 - i].lock();
                try {
                    if (slots[hash + MAX_DIST - 1 - i] != null && slots[hash + MAX_DIST - 1 - i].getKey().equals(key)) {
                        return slots[hash + MAX_DIST - 1 - i].getValue();
                    }
                } finally {
                    locks[hash + MAX_DIST - 1 - i].unlock();
                }
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public boolean containsKey(K key) {
        int hash = hash(key);
        for (int i = 0; i < MAX_DIST; i++) {
            int dist = distArray[hash].get();
            if ((dist >> i) % 2 == 1) {
                locks[hash + MAX_DIST - 1 - i].lock();
                try {
                    if (slots[hash + MAX_DIST - 1 - i] != null && slots[hash + MAX_DIST - 1 - i].getKey().equals(key)) {
                        return true;
                    }
                } finally {
                    locks[hash + MAX_DIST - 1 - i].unlock();
                }
            }
        }
        return false;
    }

    @Override
    public synchronized void put(K key, V value) {
        if (size.get() >= (slots.length * MAX_LOAD)) {
            resize();
        }
        while (true) {
            int pos = hash(key);
            int originalPos = pos;
            while (pos < slots.length - 1 && slots[pos] != null) {
                if (slots[pos].getKey().equals(key)) {
                    locks[pos].lock();
                    try {
                        slots[pos].setValue(value);
                        return;
                    }
                    finally {
                        locks[pos].unlock();
                    }
                } else
                    pos++;

            }
            if (pos <= originalPos + MAX_DIST - 1) {
                locks[pos].lock();
                try {
                    slots[pos] = new HashTableEntry<K, V>(key, value);
                    distArray[originalPos].set(distArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));//修改距离标志
                    size.getAndIncrement();
                    return;
                }
                finally {
                    locks[pos].unlock();
                }
            }
            //如果不在距离内，调整位置直至符合距离要求
            while (true) {
                boolean isSwaped = false;//设置标志判断是否调整位置成功，便于二次循环的跳转
                for (int i = MAX_DIST - 1; i > 0; i--) {
                    for (int j = MAX_DIST - 1; j > MAX_DIST - 1 - i; j--) {
                        if ((distArray[pos - i].get() >> j) % 2 == 1) {
                            locks[pos - i + MAX_DIST - 1 - j].lock();
                            locks[pos].lock();
                            try {
                                slots[pos] = slots[pos - i + MAX_DIST - 1 - j];
                                distArray[pos - i].set(distArray[pos - i].get() - (1 << j) + (1 << (MAX_DIST - i - 1))); //修改被调整值的距离标志
                                pos = pos - i + MAX_DIST - 1 - j;
                            }
                            finally {
                                locks[pos].unlock();
                                locks[pos - i + MAX_DIST - 1 - j].unlock();
                            }
                            //如果在距离内，直接插入并修改距离标志
                            if (pos <= originalPos + MAX_DIST - 1) {
                                locks[pos].lock();
                                try {
                                    slots[pos] = new HashTableEntry<K, V>(key, value);
                                    distArray[originalPos].set(distArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));//修改距离标志
                                    size.getAndIncrement();
                                    return;
                                }
                                finally {
                                    locks[pos].unlock();
                                }
                            }
                            //如果不在距离标志内
                            else {
                                isSwaped = true;
                                break;
                            }
                        }
                    }
                    if (isSwaped) {
                        break;
                    }
                }
                //如果无法调整位置
                if (!isSwaped) {
                    break;
                }
            }
            resize();
        }

    }

    private void resize() {
        boolean isSuccess = false;
        HashTableEntry<K, V>[] newSlots = new HashTableEntry[slotSize.get()];
        AtomicInteger[] newDistArray = new AtomicInteger[slotSize.get()];
        while (!isSuccess) {
            isSuccess = true;
            size.set(0);
            slotSize.set(slotSize.get() * 4);
            newSlots = new HashTableEntry[slotSize.get()];
            newDistArray = new AtomicInteger[slotSize.get()];
            for (int i = 0; i < slotSize.get(); i++) {
                newDistArray[i] = new AtomicInteger(0);
            }
            for (HashTableEntry<K, V> entry : slots) {
                if (entry != null) {
                    if (!addToSlot(entry.getKey(), entry.getValue(), newSlots, newDistArray)) {
                        isSuccess = false;
                        break;
                    }
                }
            }
        }

        ReentrantLock[] newLocks = new ReentrantLock[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            newLocks[i] = new ReentrantLock();
        }
        locks = newLocks;
        distArray = newDistArray;
        slots = newSlots;
    }

    private boolean addToSlot(K key, V value, HashTableEntry<K, V>[] newSlots, AtomicInteger[] newDistArray) {
        int pos = hash(key);
        int originalPos = pos;
        while (pos < newSlots.length - 1 && newSlots[pos] != null) {
            if (newSlots[pos].getKey().equals(key)) {
                newSlots[pos].setValue(value);
                return true;
            } else
                pos++;
        }

        if (pos <= originalPos + MAX_DIST - 1) {
            newSlots[pos] = new HashTableEntry<K, V>(key, value);
            newDistArray[originalPos].set(newDistArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));//修改距离标志
            size.getAndIncrement();
            return true;
        }
        while (true) {
            boolean isSwaped = false;//设置标志判断是否调整位置成功，便于二次循环的跳转
            for (int i = MAX_DIST - 1; i > 0; i--) {
                for (int j = MAX_DIST - 1; j > MAX_DIST - 1 - i; j--) {
                    if ((newDistArray[pos - i].get() >> j) % 2 == 1) {
                        HashTableEntry<K, V> entry = newSlots[pos - i + MAX_DIST - 1 - j];//获得需要被调整的散列位置
                        newSlots[pos] = entry;
                        newDistArray[pos - i].set(newDistArray[pos - i].get() - (1 << j) + (1 << (MAX_DIST - i - 1))); //修改被调整值的距离标志
                        pos = pos - i + MAX_DIST - 1 - j;
                        if (pos <= originalPos + MAX_DIST - 1) {
                            newSlots[pos] = new HashTableEntry<K, V>(key, value);
                            newDistArray[originalPos].set(newDistArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));//修改距离标志
                            size.getAndIncrement();
                            return true;
                        } else {
                            isSwaped = true;
                            break;
                        }
                    }
                }
                if (isSwaped) {
                    break;
                }
            }
            if (!isSwaped) {
                break;
            }
        }
        return false;
    }

    @Override
    public V remove(K key) {
        int hash = hash(key);
        for (int i = 0; i < MAX_DIST; i++) {
            int dist = distArray[hash].get();
            if ((dist >> i) % 2 == 1) {
                locks[hash + MAX_DIST - 1 - i].lock();
                try {
                    if (slots[hash + MAX_DIST - 1 - i] != null && slots[hash + MAX_DIST - 1 - i].getKey().equals(key)) {
                        int pos = hash + MAX_DIST - 1 - i;
                        V removedValue = slots[pos].getValue();
                        slots[pos] = null;
                        distArray[hash].set(distArray[hash].get() - (1 << (MAX_DIST - 1 - pos + hash)));
                        size.getAndDecrement();
                        return removedValue;
                    }
                } finally {
                    locks[hash + MAX_DIST - 1 - i].unlock();
                }
            }
        }
        return null;
    }

    @Override
    public void clear() {
        if (size.get() == 0)
            return;
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
        int hash = key.hashCode() % slotSize.get();
        if (hash < 0) {
            hash += slotSize.get();
        }
        return hash;
    }

}
