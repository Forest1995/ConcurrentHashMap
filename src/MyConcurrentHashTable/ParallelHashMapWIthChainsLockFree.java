package MyConcurrentHashTable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashMapWIthChainsLockFree<K, V> implements MyConcurrentHashTable<K, V> {
    private volatile int slotSize = 8;
    private volatile int size = 0;
    private volatile ArrayList<LinkedList<HashTableEntry<K, V>>> slots;
    private volatile AtomicBoolean[] slotStatus;
    private AtomicBoolean sizeChanging;
    private AtomicBoolean resizing;

    public ParallelHashMapWIthChainsLockFree() {
        slots = new ArrayList<>(slotSize);
        slotStatus = new AtomicBoolean[slotSize];
        sizeChanging = new AtomicBoolean();
        resizing = new AtomicBoolean();
        for (int i = 0; i < slotSize; i++) {
            slots.add(new LinkedList<HashTableEntry<K, V>>());
            slotStatus[i] = new AtomicBoolean();
            slotStatus[i].set(false);
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(K key) {
        int hash = hash(key);
        int slotIdx = hash & (slotSize - 1);
        while (true) {
            while (slotIdx >= slotStatus.length || !slotStatus[slotIdx].compareAndSet(false, true)) {

            }
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                slotStatus[oldSlotIdx].set(false);
            } else {
                break;
            }
        }


        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);
            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
                if (entry.getKey().equals(key)) {
                    //exists
                    return true;
                }
            }
            //not found
            return false;
        } catch (Exception e) {
            System.err.println("[containsKey err]" + e.getMessage());
            return false;
        } finally {
            slotStatus[slotIdx].set(false);

        }
    }


    @Override
    public V get(K key) {
        int hash = hash(key);
        int slotIdx = hash & (slotSize - 1);
        while (true) {
            while (slotIdx >= slotStatus.length || !slotStatus[slotIdx].compareAndSet(false, true)) {

            }
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                slotStatus[oldSlotIdx].set(false);
            } else {
                break;
            }
        }
        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);
            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
                if (entry.getKey().equals(key)) {
                    // exists
                    return entry.getValue();
                }
            }
            //not found
            return null;
        } finally {
            slotStatus[slotIdx].set(false);
        }
    }

    @Override
    public void put(K key, V value) {
        putVal(hash(key), key, value);
    }

    private void putVal(int hash, K key, V value) {
        int slotIdx = hash & (slotSize - 1);
        while (true) {
            while (slotIdx >= slotStatus.length || !slotStatus[slotIdx].compareAndSet(false, true)) {

            }
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                slotStatus[oldSlotIdx].set(false);
            } else {
                break;
            }
        }
        int tempSize = 0;
        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);
            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
                if (entry.getKey().equals(key)) {
                    //already exists
                    entry.setValue(value);
                    return;
                }
            }

            slot.add(new HashTableEntry<K, V>(key, value));
            slots.set(slotIdx, slot);
            while (!sizeChanging.compareAndSet(false, true)) {

            }

            size++;
            tempSize = size;
            sizeChanging.set(false);
        } catch (Exception e) {
            System.err.println("[putval err]" + slotSize + e.getMessage());
        } finally {
            slotStatus[slotIdx].set(false);
            if (tempSize > slotSize) {
                resize();
            }
        }


    }

    private void resize() {
        while (!resizing.compareAndSet(false, true)) {

        }
        try {

            while (!sizeChanging.compareAndSet(false, true)) {

            }
            if (size <= slotSize) {
                return;
            } else {
                //now size >slotSize
                sizeChanging.set(false);
                //avoid deadlock with clear()
                for (int i = 0; i < slotSize; i++) {
                    while (!slotStatus[i].compareAndSet(false, true)) {

                    }
                }
                while (!sizeChanging.compareAndSet(false, true)) {

                }
                //lock everything now
                slotSize = slotSize * 2;
                //set new locks and new slots
                AtomicBoolean[] newSlotStatus = new AtomicBoolean[slotSize];
                ArrayList<LinkedList<HashTableEntry<K, V>>> newSlots = new ArrayList<>();
                for (int i = 0; i < slotSize; i++) {
                    newSlots.add(new LinkedList<HashTableEntry<K, V>>());
                    newSlotStatus[i] = new AtomicBoolean(false);
                }
                //copy
                for (LinkedList<HashTableEntry<K, V>> slot : slots) {
                    for (HashTableEntry<K, V> entry : slot) {
                        int hash = hash(entry.getKey());
                        int slotIdx = hash & (slotSize - 1);
                        LinkedList<HashTableEntry<K, V>> targetSlot = newSlots.get(slotIdx);
                        targetSlot.add(entry);
                    }
                }
                slots = newSlots;
                slotStatus = newSlotStatus;
            }


        } finally {

            sizeChanging.set(false);
            resizing.set(false);
        }
    }

    @Override
    public V remove(K key) {
        int hash = hash(key);
        int slotIdx = hash & (slotSize - 1);
        while (true) {
            while (slotIdx >= slotStatus.length || !slotStatus[slotIdx].compareAndSet(false, true)) {

            }
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                slotStatus[oldSlotIdx].set(false);
            } else {
                break;
            }
        }
        try {
            LinkedList<HashTableEntry<K, V>> slot = slots.get(slotIdx);

            assert slot != null : "slot==null";

            for (HashTableEntry<K, V> entry : slot) {
                if (entry.getKey().equals(key)) {
                    //already exists
                    V temp = entry.getValue();
                    slot.remove(entry);
                    while (!sizeChanging.compareAndSet(false, true)) {

                    }
                    size--;
                    sizeChanging.set(false);
                    return temp;
                }
            }

            return null;
        } catch (Exception e) {
            System.err.println("[remove err]" + e.getMessage());
            return null;
        } finally {
            slotStatus[slotIdx].set(false);
        }

    }


    @Override
    public void clear() {
        if (size == 0) {
            return;
        }
        for (int i = 0; i < slotSize; i++) {
            while (!slotStatus[i].compareAndSet(false, true)) {

            }
        }
        for (int i = 0; i < slotSize; i++) {
            slots.set(i, new LinkedList<HashTableEntry<K, V>>());
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
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        int slotIdx = 0;
        for (LinkedList<HashTableEntry<K, V>> slot : slots) {
            sb.append("slot").append(slotIdx++).append(": ");
            if (slot != null) {
                for (HashTableEntry<K, V> entry : slot) {
                    sb.append("key=");
                    sb.append(entry.getKey().toString());
                    sb.append(" val=");
                    sb.append(entry.getValue().toString());
                    sb.append('|');
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
