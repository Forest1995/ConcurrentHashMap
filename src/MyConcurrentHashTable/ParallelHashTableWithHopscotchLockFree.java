package MyConcurrentHashTable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelHashTableWithHopscotchLockFree<K, V> implements MyConcurrentHashTable<K, V> {
    private volatile AtomicInteger size;
    private volatile AtomicInteger slotSize;
    private volatile HashTableEntry<K, V>[] slots;
    private volatile AtomicInteger[] distArray;
    private volatile AtomicBoolean[] slotStatus;
    private volatile AtomicBoolean putStatus;

    private static final int MAX_DIST = 30;
    private static final double MAX_LOAD = 0.1;

    public ParallelHashTableWithHopscotchLockFree() {
        size = new AtomicInteger(0);
        slotSize = new AtomicInteger(10);
        putStatus = new AtomicBoolean(false);
        slots = new HashTableEntry[slotSize.get()];
        slotStatus = new AtomicBoolean[slotSize.get()];
        distArray = new AtomicInteger[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            distArray[i] = new AtomicInteger(0);
            slotStatus[i] = new AtomicBoolean(false);
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
                while (!slotStatus[hash + MAX_DIST - 1 - i].compareAndSet(false, true)) {
                }
                try {
                    if (slots[hash + MAX_DIST - 1 - i] != null && slots[hash + MAX_DIST - 1 - i].getKey().equals(key)) {
                        return slots[hash + MAX_DIST - 1 - i].getValue();
                    }
                } finally {
                    slotStatus[hash + MAX_DIST - 1 - i].set(false);
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
                while (!slotStatus[hash + MAX_DIST - 1 - i].compareAndSet(false, true)) {
                }
                try {
                    if (slots[hash + MAX_DIST - 1 - i] != null && slots[hash + MAX_DIST - 1 - i].getKey().equals(key)) {
                        return true;
                    }
                } finally {
                    slotStatus[hash + MAX_DIST - 1 - i].set(false);
                }
            }
        }
        return false;
    }

    @Override
    public void put(K key, V value) {
        while (!putStatus.compareAndSet(false, true)) {
        }

        if (size.get() >= (slots.length * MAX_LOAD)) {
            resize();
        }
        try {
            while (true) {
                int pos = hash(key);
                int originalPos = pos;
                while (pos < slots.length - 1 && slots[pos] != null) {
                    if (slots[pos].getKey().equals(key)) {
                        while (!slotStatus[pos].compareAndSet(false, true)) {
                        }
                        try {
                            slots[pos].setValue(value);
                            return;
                        } finally {
                            slotStatus[pos].set(false);
                        }
                    } else
                        pos++;

                }
                if (pos <= originalPos + MAX_DIST - 1) {
                    while (!slotStatus[pos].compareAndSet(false, true)) {
                    }
                    try {
                        slots[pos] = new HashTableEntry<K, V>(key, value);
                        distArray[originalPos].set(distArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));
                        size.getAndIncrement();
                        return;
                    } finally {
                        slotStatus[pos].set(false);
                    }
                }
                while (true) {
                    boolean isSwaped = false;
                    for (int i = MAX_DIST - 1; i > 0; i--) {
                        for (int j = MAX_DIST - 1; j > MAX_DIST - 1 - i; j--) {
                            if ((distArray[pos - i].get() >> j) % 2 == 1) {
                                while (!slotStatus[pos - i + MAX_DIST - 1 - j].compareAndSet(false, true)) {
                                }
                                while (!slotStatus[pos].compareAndSet(false, true)) {
                                }
                                try {
                                    slots[pos] = slots[pos - i + MAX_DIST - 1 - j];
                                    distArray[pos - i].set(distArray[pos - i].get() - (1 << j) + (1 << (MAX_DIST - i - 1)));
                                    pos = pos - i + MAX_DIST - 1 - j;
                                } finally {
                                    slotStatus[pos].set(false);
                                    slotStatus[pos - i + MAX_DIST - 1 - j].set(false);
                                }
                                if (pos <= originalPos + MAX_DIST - 1) {
                                    while (!slotStatus[pos].compareAndSet(false, true)) {
                                    }
                                    try {
                                        slots[pos] = new HashTableEntry<K, V>(key, value);
                                        distArray[originalPos].set(distArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));
                                        size.getAndIncrement();
                                        return;
                                    } finally {
                                        slotStatus[pos].set(false);
                                    }
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
                resize();
            }
        } finally {
            putStatus.set(false);
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

        AtomicBoolean[] newSlotsStatus = new AtomicBoolean[slotSize.get()];
        for (int i = 0; i < slotSize.get(); i++) {
            newSlotsStatus[i] = new AtomicBoolean();
        }
        slotStatus = newSlotsStatus;
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
            newDistArray[originalPos].set(newDistArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));
            size.getAndIncrement();
            return true;
        }
        while (true) {
            boolean isSwaped = false;
            for (int i = MAX_DIST - 1; i > 0; i--) {
                for (int j = MAX_DIST - 1; j > MAX_DIST - 1 - i; j--) {
                    if ((newDistArray[pos - i].get() >> j) % 2 == 1) {
                        HashTableEntry<K, V> entry = newSlots[pos - i + MAX_DIST - 1 - j];
                        newSlots[pos] = entry;
                        newDistArray[pos - i].set(newDistArray[pos - i].get() - (1 << j) + (1 << (MAX_DIST - i - 1)));
                        pos = pos - i + MAX_DIST - 1 - j;
                        if (pos <= originalPos + MAX_DIST - 1) {
                            newSlots[pos] = new HashTableEntry<K, V>(key, value);
                            newDistArray[originalPos].set(newDistArray[originalPos].get() + (1 << (MAX_DIST - 1 - pos + originalPos)));
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
                while (!slotStatus[hash + MAX_DIST - 1 - i].compareAndSet(false, true)) {
                }
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
                    slotStatus[hash + MAX_DIST - 1 - i].set(false);
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
            while (!slotStatus[i].compareAndSet(false, true)) {
            }
        }
        for (int i = 0; i < slotSize.get(); i++) {
            slots[i] = null;
        }
        size.set(0);
        for (int i = slotSize.get() - 1; i >= 0; i--) {
            slotStatus[i].set(false);
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
