package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParallelHashTableWithCuckooLockFree<K, V> implements MyConcurrentHashTable<K, V> {

    private volatile int slotSize = 8;
    private volatile int size = 0;
    private volatile boolean reHashed = false;
    private volatile HashTableEntry<K, V>[] slots;
    private volatile AtomicBoolean[] slotStatus;
    private volatile AtomicBoolean sizeChanging;
    private volatile AtomicBoolean putStatus;
    private volatile List<HashAlgorithm> hashAlgorithmList = new ArrayList<HashAlgorithm>();
    private final int tryCount = 10;

    public ParallelHashTableWithCuckooLockFree() {
        hashAlgorithmList.add(new HashAlgorithm(12));
        hashAlgorithmList.add(new HashAlgorithm(239));
        slots = new HashTableEntry[slotSize];
        slotStatus = new AtomicBoolean[slotSize];
        sizeChanging = new AtomicBoolean();
        putStatus = new AtomicBoolean();
        for (int i = 0; i < slotSize; i++) {
            slotStatus[i] = new AtomicBoolean();
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
            while (!slotStatus[slotIdx].compareAndSet(false, true)) {

            }
            try {
                if (slots[slotIdx] != null && slots[slotIdx].getKey().equals(key))
                    return slots[slotIdx].getValue();
            } catch (Exception e) {
                System.err.println("[get err]" + e.getMessage());
                return null;
            } finally {
                slotStatus[slotIdx].set(false);
//                System.out.println("get success");

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
            while (!slotStatus[slotIdx].compareAndSet(false, true)) {

            }
            try {
                if (slots[slotIdx] != null && slots[slotIdx].getKey().equals(key))
                    return true;
            } catch (Exception e) {
                System.err.println("[containsKey err]" + e.getMessage());
                return false;
            } finally {
//                System.out.println("containskey success");
                slotStatus[slotIdx].set(false);
            }
        }
        return false;
    }

    @Override
    public void put(K key, V value) {
        while (!putStatus.compareAndSet(false, true)) {

        }
        try {
            while (true) {
                for (int i = 0; i < tryCount; i++) {
                    for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {    //遍历算法集合 计算index值，
                        int hashCode = hashAlgorithm.hashCode(key);
                        int slotIdx = hashCode & (slotSize - 1);
                        while (!slotStatus[slotIdx].compareAndSet(false, true)) {

                        }
                        try {
                            if (slots[slotIdx] == null) {
                                slots[slotIdx] = new HashTableEntry(key, value);//当表中索引无值，将元素放到表中
                                while (!sizeChanging.compareAndSet(false, true)) {

                                }
                                size++;
                                sizeChanging.set(false);
                                return;
                            }
                        } catch (Exception e) {
                            System.err.println("[put err]" + e.getMessage());
                            return;
                        } finally {
                            slotStatus[slotIdx].set(false);
                        }
                    }

                    //执行到这说明 每个位置都有人 进行替换操作

                    int hashAlgorithmListIndex = new Random().nextInt(hashAlgorithmList.size());//随机选取一个函数
                    int hashCode = hashAlgorithmList.get(hashAlgorithmListIndex).hashCode(key);
                    int slotIdx = hashCode & (slotSize - 1);

                    while (!slotStatus[slotIdx].compareAndSet(false, true)) {

                    }
                    try {
                        K oldKey = slots[slotIdx].getKey();                //原本表中这个索引对应的entry
                        V oldValue = slots[slotIdx].getValue();
                        slots[slotIdx] = new HashTableEntry(key, value);   //把要插入的entry 放到当前位置上
                        key = oldKey;
                        value = oldValue;                                 //现在就是要插入原来替换掉的值
                    } finally {
                        slotStatus[slotIdx].set(false);
                    }

                }

                if (reHashed || size >= slots.length) {               //说明要进行扩容操作了
                    System.out.println("resize");
                    reSize();
                    reHashed = false;
                } else {
                    System.out.println("rehash");
                    ReHash();                                           //重新计算hash值
                    reHashed = true;
                }
            }
        }
        finally {
//            System.out.println("put success");

            putStatus.set(false);
        }
    }

    @Override
    public V remove(K key) {
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode & (slotSize - 1);
            while (!slotStatus[slotIdx].compareAndSet(false, true)) {

            }
            try {
                if (slots[slotIdx] != null && slots[slotIdx].getKey().equals(key)) {
                    V removedValue = slots[slotIdx].getValue();
                    slots[slotIdx] = null;
                    while (!sizeChanging.compareAndSet(false, true)) {

                    }
                    size--;
                    sizeChanging.set(false);
                    return removedValue;
                }
            } catch (Exception e) {
                System.err.println("[remove err]" + e.getMessage());
                return null;

            } finally {
//                System.out.println("remove success");
                slotStatus[slotIdx].set(false);
            }
        }
        return null;
    }

    @Override
    public void clear() {
        for (int i = 0; i < slotSize; i++) {
            while (!slotStatus[i].compareAndSet(false, true)) {

            }
        }
        for (int i = 0; i < slotSize; i++) {
            slots[i] = null;
        }
        while (!sizeChanging.compareAndSet(false, true)) {

        }
        size = 0;
        sizeChanging.set(false);
        for (int i = slotSize - 1; i >= 0; i--) {
            slotStatus[i].set(false);
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
        slotStatus = new AtomicBoolean[slotSize];
        for (int i = 0; i < slotSize; i++) {
            slotStatus[i] = new AtomicBoolean();
        }
        ReAssignSlots();
    }

    private void ReAssignSlots() {
        HashTableEntry<K, V>[] oldSlots = slots;
        slots = new HashTableEntry[slotSize];
        while (!sizeChanging.compareAndSet(false, true)) {

        }
        size = 0;
        sizeChanging.set(false);
        for (HashTableEntry<K, V> entry : oldSlots) {
            if (entry != null)
                put2(entry.getKey(), entry.getValue());
        }

    }

    private void put2(K key, V value){
        while (true) {
            for (int i = 0; i < tryCount; i++) {
                for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {    //遍历算法集合 计算index值，
                    int hashCode = hashAlgorithm.hashCode(key);
                    int slotIdx = hashCode & (slotSize - 1);
                    while (!slotStatus[slotIdx].compareAndSet(false, true)) {

                    }
                    try {
                        if (slots[slotIdx] == null) {
                            slots[slotIdx] = new HashTableEntry(key, value);//当表中索引无值，将元素放到表中
                            while (!sizeChanging.compareAndSet(false, true)) {

                            }
                            size++;
                            sizeChanging.set(false);
                            return;
                        }
                    } catch (Exception e) {
                        System.err.println("[put err]" + e.getMessage());
                        return;
                    } finally {
                        slotStatus[slotIdx].set(false);
                    }
                }

                //执行到这说明 每个位置都有人 进行替换操作

                int hashAlgorithmListIndex = new Random().nextInt(hashAlgorithmList.size());//随机选取一个函数
                int hashCode = hashAlgorithmList.get(hashAlgorithmListIndex).hashCode(key);
                int slotIdx = hashCode & (slotSize - 1);

                while (!slotStatus[slotIdx].compareAndSet(false, true)) {

                }
                try {
                    K oldKey = slots[slotIdx].getKey();                //原本表中这个索引对应的entry
                    V oldValue = slots[slotIdx].getValue();
                    slots[slotIdx] = new HashTableEntry(key, value);   //把要插入的entry 放到当前位置上
                    key = oldKey;
                    value = oldValue;                                 //现在就是要插入原来替换掉的值
                } finally {
                    slotStatus[slotIdx].set(false);
                }

            }
            if (reHashed || size >= slots.length) {               //说明要进行扩容操作了
                reSize();
                reHashed = false;
            } else {
                ReHash();                                           //重新计算hash值
                reHashed = true;
            }
        }
    }
}
