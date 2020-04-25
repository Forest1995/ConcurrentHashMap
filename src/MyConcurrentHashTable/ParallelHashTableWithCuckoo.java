package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashTableWithCuckoo<K, V> implements MyConcurrentHashTable<K, V> {

    private volatile AtomicInteger size;
    private volatile AtomicInteger slotSize;
    private volatile AtomicBoolean reHashed;
    private volatile HashTableEntry<K, V>[] slots;
    private volatile ReentrantLock[] locks;
    private volatile List<HashAlgorithm> hashAlgorithmList = new ArrayList<HashAlgorithm>();
    private static final int tryCount = 17;
    private static final double MAX_LOAD = 0.5;


    public ParallelHashTableWithCuckoo() {
        size = new AtomicInteger(0);
        slotSize = new AtomicInteger(8);
        hashAlgorithmList.add(new HashAlgorithm(10));
        hashAlgorithmList.add(new HashAlgorithm(23));
        slots = new HashTableEntry[slotSize.get()];
        locks = new ReentrantLock[slotSize.get()];
        reHashed = new AtomicBoolean(false);
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
            int slotIdx = hashCode & (slotSize.get() - 1);

            while (true) {
                //until get correct lock
                while (slotIdx >= locks.length || !locks[slotIdx].tryLock()) {
                }
                //get one lock, we can safely check the slot now, because resize() cannot execute.
                int oldSlotIdx = slotIdx;
                int curSlotIdx = hashCode & (slotSize.get() - 1);
                //if not correct,unlock and try again.
                if (slotIdx != curSlotIdx) { //caused by resizing.
                    slotIdx = curSlotIdx;
//                while(slotIdx>=locks.length||!locks[slotIdx].tryLock()){}
                    //lock new slot and then unlock old. To avoid resizing.
                    locks[oldSlotIdx].unlock();
                } else {
                    break;
                }
            }
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
        return size.get()== 0;
    }

    @Override
    public boolean containsKey(K key) {
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode & (slotSize.get() - 1);
            while (true) {
                //until get correct lock
                while (slotIdx >= locks.length || !locks[slotIdx].tryLock()) {
                }
                //get one lock, we can safely check the slot now, because resize() cannot execute.
                int oldSlotIdx = slotIdx;
                int curSlotIdx = hashCode & (slotSize.get() - 1);
                //if not correct,unlock and try again.
                if (slotIdx != curSlotIdx) { //caused by resizing.
                    slotIdx = curSlotIdx;
//                while(slotIdx>=locks.length||!locks[slotIdx].tryLock()){}
                    //lock new slot and then unlock old. To avoid resizing.
                    locks[oldSlotIdx].unlock();
                } else {
                    break;
                }
            }
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
    public  void put(K key, V value) {
        while (true) {
            for (int i = 0; i < tryCount; i++) {
                for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {    //遍历算法集合 计算index值，
                    int hashCode = hashAlgorithm.hashCode(key);
                    int slotIdx = hashCode & (slotSize.get() - 1);
                    while(true){
                        //until get correct lock
                        while(slotIdx>=locks.length||!locks[slotIdx].tryLock()){}
                        //get one lock, we can safely check the slot now, because resize() cannot execute.
                        int oldSlotIdx=slotIdx;
                        int curSlotIdx=hashCode&(slotSize.get()-1);
                        //if not correct,unlock and try again.
                        if(slotIdx!=curSlotIdx){ //caused by resizing.
                            slotIdx=curSlotIdx;
//                while(slotIdx>=locks.length||!locks[slotIdx].tryLock()){}
                            //lock new slot and then unlock old. To avoid resizing.
                            locks[oldSlotIdx].unlock();

                        }else{
                            break;
                        }
                    }
                    try {
                        if (slots[slotIdx] == null) {
                            slots[slotIdx] = new HashTableEntry(key, value);//当表中索引无值，将元素放到表中
                            size.getAndIncrement();
                            return;
                        }
                    } finally {
                        locks[slotIdx].unlock();
                    }
                }

                //执行到这说明 每个位置都有人 进行替换操作

                int hashAlgorithmListIndex = new Random().nextInt(hashAlgorithmList.size());//随机选取一个函数
                int hashCode = hashAlgorithmList.get(hashAlgorithmListIndex).hashCode(key);
                int slotIdx = hashCode & (slotSize.get() - 1);

                while(true){
                    //until get correct lock
                    while(slotIdx>=locks.length||!locks[slotIdx].tryLock()){}
                    //get one lock, we can safely check the slot now, because resize() cannot execute.
                    int oldSlotIdx=slotIdx;
                    int curSlotIdx=hashCode&(slotSize.get()-1);
                    //if not correct,unlock and try again.
                    if(slotIdx!=curSlotIdx){ //caused by resizing.
                        slotIdx=curSlotIdx;
//                while(slotIdx>=locks.length||!locks[slotIdx].tryLock()){}
                        //lock new slot and then unlock old. To avoid resizing.
                        locks[oldSlotIdx].unlock();

                    }else{
                        break;
                    }
                }

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

            if (reHashed.get() || (size.get() >= slotSize.get()*MAX_LOAD)) {               //说明要进行扩容操作了
                reSize();
                reHashed.set(false);
            } else {
                ReHash();                                           //重新计算hash值
                reHashed .set(true);
            }
        }
    }

    @Override
    public V remove(K key) {
        for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {
            int hashCode = hashAlgorithm.hashCode(key);
            int slotIdx = hashCode & (slotSize.get() - 1);

            while (true) {
                //until get correct lock
                while (slotIdx >= locks.length || !locks[slotIdx].tryLock()) {
                }
                //get one lock, we can safely check the slot now, because resize() cannot execute.
                int oldSlotIdx = slotIdx;
                int curSlotIdx = hashCode & (slotSize.get() - 1);
                //if not correct,unlock and try again.
                if (slotIdx != curSlotIdx) { //caused by resizing.
                    slotIdx = curSlotIdx;
//                while(slotIdx>=locks.length||!locks[slotIdx].tryLock()){}
                    //lock new slot and then unlock old. To avoid resizing.
                    locks[oldSlotIdx].unlock();
                } else {
                    break;
                }
            }
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


    private void ReHash() {
        for (int i = 0; i < slotSize.get(); i++) {
            locks[i].lock();
        }
        hashAlgorithmList.clear();
        int one = new Random().nextInt(100);
        int two = new Random().nextInt(100);
        two = one == two ? two * 2 : two;
        hashAlgorithmList.add(new HashAlgorithm(one));
        hashAlgorithmList.add(new HashAlgorithm(two));
        ReAssignSlots();
    }

    private void reSize() {
        for (int i = 0; i < slotSize.get(); i++) {
            locks[i].lock();
        }
        slotSize = new AtomicInteger(slotSize.get()*2);
        locks = new ReentrantLock[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            locks[i] = new ReentrantLock();
        }
        ReAssignSlots();
    }

    private void ReAssignSlots() {
        HashTableEntry<K, V>[] oldSlots = slots;
        slots = new HashTableEntry[slotSize.get()];
        size.set(0);
        for (HashTableEntry<K, V> entry : oldSlots) {
            if (entry != null)
                singleThreadPut(entry.getKey(), entry.getValue());
        }
    }

    private void singleThreadPut(K key, V value) {
        while (true) {
            for (int i = 0; i < tryCount; i++) {
                for (HashAlgorithm hashAlgorithm : hashAlgorithmList) {    //遍历算法集合 计算index值，
                    int hashCode = hashAlgorithm.hashCode(key);
                    int slotIdx = hashCode & (slotSize.get() - 1);
                    if (slots[slotIdx] == null) {
                        slots[slotIdx] = new HashTableEntry(key, value);//当表中索引无值，将元素放到表中
                        size.getAndIncrement();
                        return;
                    }

                }
                //执行到这说明 每个位置都有人 进行替换操作

                int hashAlgorithmListIndex = new Random().nextInt(hashAlgorithmList.size());//随机选取一个函数
                int hashCode = hashAlgorithmList.get(hashAlgorithmListIndex).hashCode(key);
                int slotIdx = hashCode & (slotSize.get() - 1);

                K oldKey = slots[slotIdx].getKey();                //原本表中这个索引对应的entry
                V oldValue = slots[slotIdx].getValue();
                slots[slotIdx] = new HashTableEntry(key, value);   //把要插入的entry 放到当前位置上
                key = oldKey;
                value = oldValue;                                 //现在就是要插入原来替换掉的值
            }

            singleThreadReHash();
        }
    }

    private void singleThreadReHash() {
        hashAlgorithmList.clear();
        int one = new Random().nextInt(100);
        int two = new Random().nextInt(100);
        two = one == two ? two * 2 : two;
        hashAlgorithmList.add(new HashAlgorithm(one));
        hashAlgorithmList.add(new HashAlgorithm(two));
        ReAssignSlots();
    }


}
