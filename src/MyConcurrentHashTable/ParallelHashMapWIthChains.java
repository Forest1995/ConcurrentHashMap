package MyConcurrentHashTable;


import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelHashMapWIthChains<K, V> implements MyConcurrentHashTable<K, V> {
    private volatile int slotSize = 8;
    private volatile int size = 0;
    private volatile ArrayList<LinkedList<HashTableEntry<K, V>>> slots;
    private volatile ReentrantLock[] locks;
    private ReentrantLock sizeLock;
    private ReentrantLock resizeLock;

    public ParallelHashMapWIthChains() {
        slots = new ArrayList<>(slotSize);
        locks = new ReentrantLock[slotSize];
        sizeLock = new ReentrantLock();
        resizeLock = new ReentrantLock();
        for (int i = 0; i < slotSize; i++) {
            slots.add(new LinkedList<HashTableEntry<K, V>>());
            locks[i] = new ReentrantLock();
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
            //loop until get correct lock
            while (slotIdx >= locks.length || !locks[slotIdx].tryLock()) {
            }
            //now we get one lock, we can safely check the slot now, because resize() cannot execute.
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            //slotIdx can be different from current slotIdx. If this is the case, loop again.
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                locks[oldSlotIdx].unlock();//unlock wrong slotIdx
            } else {
                //it didn't change. safe.
                break;
            }
        }
        //now the slotIdx must be correct. And we have the lock.

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
            locks[slotIdx].unlock();

        }
    }


    @Override
    public V get(K key) {
        int hash = hash(key);
        int slotIdx = hash & (slotSize - 1);
        while (true) {
            //until get correct lock
            while (slotIdx >= locks.length || !locks[slotIdx].tryLock()) {
            }
            //get one lock, we can safely check the slot now, because resize() cannot execute.
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            //if not correct,unlock and try again.
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                //lock new slot and then unlock old. To avoid resizing.
                locks[oldSlotIdx].unlock();
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
            locks[slotIdx].unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        putVal(hash(key), key, value);
    }

    private void putVal(int hash, K key, V value) {
        int slotIdx = hash & (slotSize - 1);
        while (true) {
            //until get correct lock
            while (slotIdx >= locks.length || !locks[slotIdx].tryLock()) {
            }
            //get one lock, we can safely check the slot now, because resize() cannot execute.
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            //if not correct,unlock and try again.
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                //lock new slot and then unlock old. To avoid resizing.
                locks[oldSlotIdx].unlock();

            } else {
                break;
            }
        }

        //get lock now

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
            sizeLock.lock();
            size++;
            tempSize = size;
            sizeLock.unlock();
        } catch (Exception e) {
            System.err.println("[putval err]" + slotSize + e.getMessage());
        } finally {
            locks[slotIdx].unlock();
            if (tempSize > slotSize) {
                resize();
            }
        }


    }

    private void resize() {
        resizeLock.lock();
        try {

            sizeLock.lock();
            if (size <= slotSize) {
                //maybe many threads enter this function at the same time.
                //when one thread succeeds in resize() and unlock, all other threads should not resize() again.
                return;
            } else {
                //now size >slotSize, only one thread should do this.
                sizeLock.unlock();
                //avoid deadlock with clear()
                for (int i = 0; i < slotSize; i++) {
                    locks[i].lock();
                }
                sizeLock.lock();
                //lock [everything] now, including slot lock, sizelock and resizelock. Other threads cannot do anything.
                slotSize = slotSize * 2;
                //set new locks and new slots
                ReentrantLock[] newLocks = new ReentrantLock[slotSize];
                ArrayList<LinkedList<HashTableEntry<K, V>>> newSlots = new ArrayList<>();
                for (int i = 0; i < slotSize; i++) {
                    newSlots.add(new LinkedList<HashTableEntry<K, V>>());
                    newLocks[i] = new ReentrantLock();
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
                locks = newLocks;
            }


        } finally {

            sizeLock.unlock();
            resizeLock.unlock();
        }
    }

    @Override
    public V remove(K key) {
        int hash = hash(key);
        int slotIdx = hash & (slotSize - 1);
        while (true) {
            //until get correct lock
            while (slotIdx >= locks.length || !locks[slotIdx].tryLock()) {
            }
            //get one lock, we can safely check the slot now, because resize() cannot execute.
            int oldSlotIdx = slotIdx;
            int curSlotIdx = hash & (slotSize - 1);
            //if not correct,unlock and try again.
            if (slotIdx != curSlotIdx) { //caused by resizing.
                slotIdx = curSlotIdx;
                //lock new slot and then unlock old. To avoid resizing.
                locks[oldSlotIdx].unlock();

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
                    sizeLock.lock();
                    size--;
                    sizeLock.unlock();
                    return temp;
                }
            }

            return null;
        } catch (Exception e) {
            System.err.println("[remove err]" + e.getMessage());
            return null;
        } finally {
            locks[slotIdx].unlock();
        }

    }


    @Override
    public void clear() {
        if (size == 0) {
            return;
        }
        for (int i = 0; i < slotSize; i++) {
            locks[i].lock();
        }
        for (int i = 0; i < slotSize; i++) {
            slots.set(i, new LinkedList<HashTableEntry<K, V>>());
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
