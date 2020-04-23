package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithCuckoo<K, V> implements MyConcurrentHashTable<K, V> {

    private int slotSize = 8;
    private int size = 0;
    private boolean reHashed = false;
    private HashTableEntry<K, V>[] slots;
    private ReentrantLock[] locks;
    private ReentrantLock sizeLock;
    private ReentrantLock reHashLock;
    private List<HashAlgorithm> hashAlgorithmList = new ArrayList<HashAlgorithm>();
    private final int tryCount = 10;


    public ParallelHashTableWithCuckoo() {
        hashAlgorithmList.add(new HashAlgorithm(10));
        hashAlgorithmList.add(new HashAlgorithm(23));
        slots = new HashTableEntry[slotSize];
        locks = new ReentrantLock[slotSize];
        reHashLock = new ReentrantLock();
        sizeLock = new ReentrantLock();
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
    public void put(K key, V value) {
        while (true) {
            for (int i = 0; i < tryCount; i++) {
                for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {    //遍历算法集合 计算index值，
                    int hashCode = hashAlgorithm.hashCode(key);
                    int slotIdx = hashCode & (slotSize - 1);
                    locks[slotIdx].lock();
                    try {
                        if (slots[slotIdx] == null) {
                            slots[slotIdx] = new HashTableEntry(key, value);//当表中索引无值，将元素放到表中
                            sizeLock.lock();
                            size++;
                            sizeLock.unlock();
                            return;
                        }
                    } finally {
                        locks[slotIdx].unlock();
                    }
                }

                //执行到这说明 每个位置都有人 进行替换操作

                int hashAlgorithmListIndex = new Random().nextInt(hashAlgorithmList.size());//随机选取一个函数
                int hashCode = hashAlgorithmList.get(hashAlgorithmListIndex).hashCode(key);
                int slotIdx = hashCode & (slotSize - 1);

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

            reHashLock.lock();
            try {
                if (reHashed || size >= slots.length) {               //说明要进行扩容操作了
                    reSize();
                    reHashed = false;
                } else {
                    ReHash();                                           //重新计算hash值
                    reHashed = true;
                }
                return;
            } finally {
                reHashLock.unlock();
            }
        }
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
                    sizeLock.lock();
                    size--;
                    sizeLock.unlock();
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
        for (int i = 0; i < slotSize; i++) {
            locks[i].lock();
        }
        for (int i = 0; i < slotSize; i++) {
            slots[i] = null;
        }
        sizeLock.lock();
        size = 0;
        sizeLock.unlock();
        for (int i = slotSize - 1; i >= 0; i--) {
            locks[i].unlock();
        }
    }

    @Override
    public int hash(K key) {
        return 0;
    }


    private void ReHash() {
        hashAlgorithmList.clear();
        int one = new Random().nextInt(1000);
        int two = new Random().nextInt(1000);
        two = one == two ? two * 2 : two;
        hashAlgorithmList.add(new HashAlgorithm(one));
        hashAlgorithmList.add(new HashAlgorithm(two));
        ReAssignSlots();
    }

    private void reSize() {
        slotSize = 2 * slotSize;
        ReAssignSlots();
    }

    private void ReAssignSlots() {
        HashTableEntry<K, V>[] oldSlots = slots;
        slots = new HashTableEntry[slotSize];
        size = 0;
        for (HashTableEntry<K, V> entry : oldSlots) {
            if (entry != null)
                put(entry.getKey(), entry.getValue());
        }

    }
}
