package MyConcurrentHashTable;

class HashAlgorithm <K> {
    private int initNumber;
    public HashAlgorithm(int initNumber) {
        this.initNumber = initNumber;
    }

    public int hashCode(K key) {
        return  key.hashCode()+ initNumber ;
    }
}