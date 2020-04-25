package MyConcurrentHashTable;

public class HashTableEntry<K,V> {
    private K key;
    private V value;
    private int dist; // for hopscotch
    HashTableEntry(K key, V value){
        this.key= key;
        this.value=value;
    }

    HashTableEntry(K key, V value, int dist){
        this.key = key;
        this.value = value;
        this.dist = dist;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public void setDist(int dist){this.dist =dist;}

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public int getDist(){return dist;}
}
