package heap;

public class Tuple {

    int length;
    byte[] data;
    
    public Tuple(byte [] data , int offset, int length)
    {
        this.data = data;
        this.length = length;
    }

    public Tuple(int length, byte[] data) {
        this.length = length;
        this.data = data;
    }

    public Tuple()
    {

    }
    public int getLength()
    {
        return 0;
    }

    public byte[] getTupleByteArray()
    {
        return new byte[1];
    }


}
