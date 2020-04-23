package MyConcurrentHashTable;
public interface MyConcurrentHashTable<K,V> {
    int size();
    V get(K key);
    boolean isEmpty();
    boolean containsKey(K key);
    void put(K key, V value);
    V remove(K key);
    void clear();
    int hash(K key);
}
