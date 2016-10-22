package heap;

public class HeapScan {

    protected HeapScan(HeapFile hf){};
    protected void finalize() throws Throwable{};
    public void close(){};
    public boolean hasNext(){return false;};
    public Tuple getNext(RID rid){return new Tuple();};

}
