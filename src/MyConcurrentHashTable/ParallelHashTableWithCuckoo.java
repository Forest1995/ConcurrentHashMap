package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithCuckoo<K, V> implements MyConcurrentHashTable<K, V> {

    private volatile AtomicInteger slotSize ;
    private volatile AtomicInteger size ;
    private volatile HashTableEntry<K, V>[] slots;
    private volatile ReentrantLock[] locks;
    private volatile ReentrantLock sizeLock;
    private volatile List<HashAlgorithm> hashAlgorithmList = new ArrayList<HashAlgorithm>();
    private final int tryCount = 23;
    private static final double MAX_LOAD = 0.1;

    public ParallelHashTableWithCuckoo() {
        hashAlgorithmList.add(new HashAlgorithm(126001));
        hashAlgorithmList.add(new HashAlgorithm(42589));
        hashAlgorithmList.add(new HashAlgorithm(27361));
        hashAlgorithmList.add(new HashAlgorithm(3626149));
        slotSize = new AtomicInteger(10);
        size = new AtomicInteger(0);
        slots = new HashTableEntry[slotSize.get()];
        locks = new ReentrantLock[slotSize.get()];
        sizeLock = new ReentrantLock();
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
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode % slotSize.get();
            if (slotIdx < 0) {
                slotIdx += slotSize.get();
            }
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
        return size.get() == 0;
    }

    @Override
    public boolean containsKey(K key) {
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode % slotSize.get();
            if (slotIdx < 0) {
                slotIdx += slotSize.get();
            }
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
    public synchronized void put(K key, V value) {
        if (size.get() >=  slots.length * MAX_LOAD)
            resize();
        while (true) {
            for (int i = 0; i < tryCount; i++) {
                for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {    //遍历算法集合 计算index值，
                    int hashCode = hashAlgorithm.hashCode(key);
                    int slotIdx = hashCode % slotSize.get();
                    if (slotIdx < 0) {
                        slotIdx += slotSize.get();
                    }
                    locks[slotIdx].lock();
                    try {
                        if (slots[slotIdx] == null) {
                            slots[slotIdx] = new HashTableEntry(key, value);//当表中索引无值，将元素放到表中
                            size.getAndIncrement();
                            return;
                        } else {
                            if (slots[slotIdx].getKey().equals(key)) {
                                slots[slotIdx].setValue(value);
                                return;
                            }
                        }
                    } finally {
                        locks[slotIdx].unlock();
                    }
                }

                //执行到这说明 每个位置都有人 进行替换操作

                int hashAlgorithmListIndex = new Random().nextInt(hashAlgorithmList.size());//随机选取一个函数
                int hashCode = hashAlgorithmList.get(hashAlgorithmListIndex).hashCode(key);
                int slotIdx = hashCode % slotSize.get();
                if (slotIdx < 0) {
                    slotIdx += slotSize.get();
                }

                locks[slotIdx].lock();
                try {
                    K oldKey = slots[slotIdx].getKey();                //原本表中这个索引对应的entry
                    V oldValue = slots[slotIdx].getValue();
                    slots[slotIdx] = new HashTableEntry(key, value);   //把要插入的entry 放到当前位置上
                    key = oldKey;
                    value = oldValue;                                 //现在就是要插入原来替换掉的值
                } finally {
                    locks[slotIdx].unlock();
                }

            }
            boolean success = rehash();
            if(!success){
                resize();
            }
        }
    }

    @Override
    public V remove(K key) {
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode % slotSize.get();
            if (slotIdx < 0) {
                slotIdx += slotSize.get();
            }
            locks[slotIdx].lock();
            try {
                if (slots[slotIdx] != null && slots[slotIdx].getKey().equals(key)) {
                    V removedValue = slots[slotIdx].getValue();
                    slots[slotIdx] = null;
                    size.getAndDecrement();
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
        if(size.get()==0){
            return;
        }
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
        return 0;
    }


    private boolean rehash() {
        hashAlgorithmList.clear();
        int one = new Random().nextInt();
        int two = new Random().nextInt();
        two = one == two ? two * 2 : two;
        hashAlgorithmList.add(new HashAlgorithm(181081));
        hashAlgorithmList.add(new HashAlgorithm(374321));
        hashAlgorithmList.add(new HashAlgorithm(one));
        hashAlgorithmList.add(new HashAlgorithm(two));

        HashTableEntry <K,V>[] newSlots = new HashTableEntry[slotSize.get()];
        size.set(0);
        for (HashTableEntry<K, V> entry : slots) {
            if (entry != null)
                if(!addToSlot(entry.getKey(), entry.getValue(), newSlots)){
                    return false;
                }
        }
        slots = newSlots;
        return true;
    }

    private void resize() {
        slotSize.set(4 * slotSize.get());
        ReentrantLock [] newLocks = new ReentrantLock[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            newLocks[i] = new ReentrantLock();
        }
        HashTableEntry <K,V>[] newSlots = new HashTableEntry[slotSize.get()];
        size.set(0);
        for (HashTableEntry<K, V> entry : slots) {
            if (entry != null)
                addToSlot(entry.getKey(), entry.getValue(), newSlots);
        }
        slots = newSlots;
        locks = newLocks;
    }


    private boolean addToSlot(K key, V value, HashTableEntry<K,V> [] newSlots) {
        for (int i = 0; i < tryCount; i++) {
            for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {    //遍历算法集合 计算index值，
                int hashCode = hashAlgorithm.hashCode(key);
                int slotIdx = hashCode % slotSize.get();
                if (slotIdx < 0) {
                    slotIdx += slotSize.get();
                }
                if (newSlots[slotIdx] == null) {
                    newSlots[slotIdx] = new HashTableEntry(key, value);//当表中索引无值，将元素放到表中
                    size.getAndIncrement();
                    return true;
                } else {
                    if (newSlots[slotIdx].getKey().equals(key)) {
                        newSlots[slotIdx].setValue(value);
                        return true;
                    }
                }
            }
            //执行到这说明 每个位置都有人 进行替换操作
            int hashAlgorithmListIndex = new Random().nextInt(hashAlgorithmList.size());//随机选取一个函数
            int hashCode = hashAlgorithmList.get(hashAlgorithmListIndex).hashCode(key);
            int slotIdx = hashCode % slotSize.get();
            if (slotIdx < 0) {
                slotIdx += slotSize.get();
            }

            K oldKey = newSlots[slotIdx].getKey();                //原本表中这个索引对应的entry
            V oldValue = newSlots[slotIdx].getValue();
            newSlots[slotIdx] = new HashTableEntry(key, value);   //把要插入的entry 放到当前位置上
            key = oldKey;
            value = oldValue;                                 //现在就是要插入原来替换掉的值
        }
        return false;
    }
}