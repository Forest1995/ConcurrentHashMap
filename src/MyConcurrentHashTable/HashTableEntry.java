package MyConcurrentHashTable;

public class HashTableEntry<K,V> {
    private K key;
    private V value;
    HashTableEntry(K key, V value){
        this.key= key;
        this.value=value;
    }


    public void setKey(K key) {
        this.key = key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

}
