package MyConcurrentHashTable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithHopscotchLockFree <K, V> implements MyConcurrentHashTable<K, V> {
    private volatile AtomicInteger size;
    private volatile AtomicInteger slotSize;
    private volatile HashTableEntry<K, V>[] slots;
    private volatile ReentrantLock[] locks;
    private volatile AtomicInteger[] distArray;
    private volatile ReentrantLock resizeLock;
    private static final int MAX_DIST = 4;
    private static final double MAX_LOAD = 0.5;

    public ParallelHashTableWithHopscotchLockFree() {
        size = new AtomicInteger(0);
        slotSize = new AtomicInteger(8);
        slots = new HashTableEntry[slotSize.get()];
        locks = new ReentrantLock[slotSize.get()];
        distArray = new AtomicInteger[slotSize.get()];
        resizeLock = new ReentrantLock();
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
        int pos = findPos(key);
        if (pos != -1) {
            return slots[pos].getValue();
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public boolean containsKey(K key) {
        return findPos(key) != -1;
    }

    @Override
    public void put(K key, V value) {
        //1.如果元素数目达到装载极限
        if (size.get() >= (int) (slotSize.get() * MAX_LOAD)) {
            resize();//再散列，即扩容
        }
        //2.不断循环直至插入成功
        insert(key, value);
    }

    private synchronized void insert(K key, V value) {
        while (true) {
            //获取散列位置
            int pos = hash(key);
            //保存最初散列值
            int originalPos = pos;
            //循环以得到空位
            while (pos < slotSize.get() - 1 && slots[pos] != null) {
                if (slots[pos].getKey().equals(key)) {
                    slots[pos].setValue(value);
                    return;
                } else
                    pos++;
            }
//            locks[pos].lock();
//            try {
//                if (slots[pos] != null)
//                    continue;

            //如果空位在距离内，直接插入并修改距离标志
            if (pos <= originalPos + MAX_DIST - 1) {
                slots[pos] = new HashTableEntry<K, V>(key, value);
                distArray[originalPos].set(distArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));//修改距离标志
                size.getAndIncrement();
                return;
            }
            //如果不在距离内，调整位置直至符合距离要求
            while (true) {
                boolean isSwaped = false;//设置标志判断是否调整位置成功，便于二次循环的跳转
                //散列位置从最远处开始
                for (int i = MAX_DIST - 1; i > 0; i--) {
                    //距离标志从最高位开始
                    for (int j = MAX_DIST - 1; j > MAX_DIST - 1 - i; j--) {
                        //如果距离标志位为1，则可以调整位置
                        if ((distArray[pos - i].get() >> j) % 2 == 1) {
                            HashTableEntry<K, V> entry = slots[pos - i + MAX_DIST - 1 - j];//获得需要被调整的散列位置
                            slots[pos] = entry;
                            distArray[pos - i].set(distArray[pos - i].get() - (1 << j) + (1 << (MAX_DIST - i - 1))); //修改被调整值的距离标志
                            pos = pos - i + MAX_DIST - 1 - j;
                            //如果在距离内，直接插入并修改距离标志
                            if (pos <= originalPos + MAX_DIST - 1) {
                                slots[pos] = new HashTableEntry<K, V>(key, value);
                                distArray[originalPos].set(distArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));//修改距离标志
                                size.getAndIncrement();
                                return;
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
            //再散列，重新开始插入
            resize();
        }
//            finally {
//                locks[pos].unlock();
//            }
//        }
    }

    private void resize() {
//        resizeLock.lock();
//        try {
//

//        for (int i = 0; i < slotSize.get(); i++) {
//            locks[i].lock();
//        }
        HashTableEntry<K, V>[] oldSlots = slots;
        slotSize = new AtomicInteger(slotSize.get() * 2);
        slots = new HashTableEntry[slotSize.get()];
        distArray = new AtomicInteger[slotSize.get()];
        locks = new ReentrantLock[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            locks[i] = new ReentrantLock();
            distArray[i] = new AtomicInteger(0);
        }
        size.set(0);
        for (HashTableEntry<K, V> entry : oldSlots) {
            if (entry != null) {
                put(entry.getKey(), entry.getValue());
            }
        }
//        } finally {
//            resizeLock.unlock();
//        }
    }

    @Override
    public V remove(K key) {
        int pos = findPos(key);
        int hash = hash(key);
        if (pos != -1) {
//            locks[pos].lock();
//            try {
            V removedValue = slots[pos].getValue();
            slots[pos] = null;
            distArray[hash].set(distArray[hash].get() - (1 << (MAX_DIST - 1 - pos + hash)));
            size.getAndDecrement();
            return removedValue;
//            } finally {
//                locks[pos].unlock();
//            }
        }
        return null;
    }

    @Override
    public void clear() {
//        for (int i = 0; i < slotSize.get(); i++) {
//            locks[i].lock();
//        }
        for (int i = 0; i < slotSize.get(); i++) {
            slots[i] = null;
        }
        size.set(0);
//        for (int i = slotSize.get() - 1; i >= 0; i--) {
//            locks[i].unlock();
//        }
    }

    @Override
    public int hash(K key) {
        int hash = key.hashCode() % slotSize.get();
        if (hash < 0) {
            hash += slotSize.get();
        }
        return hash;
    }

    private int findPos(K key) {
        int hash = hash(key);
        for (int i = 0; i < MAX_DIST; i++) {
            int dist = distArray[hash].get();
            if ((dist >> i) % 2 == 1) {
//                locks[hash + MAX_DIST - 1 - i].lock();
//                try {
                if (slots[hash + MAX_DIST - 1 - i].getKey().equals(key)) {
                    return hash + MAX_DIST - 1 - i;
                }
//                } finally {
//                    locks[hash + MAX_DIST - 1 - i].unlock();
//                }

            }
        }
        return -1;
    }
}