package heap;

import java.util.Arrays;

public class Tuple {

    int length;
    byte[] data;
    
    @Override
	public String toString() {
		return "Tuple [length=" + length + ", data=" + Arrays.toString(data) + "]";
	}

	public Tuple(byte [] data , int offset, int length)
    {
        this.data = data;
        this.length = length;
    }

    public Tuple(byte[] data) {
        this.length = data.length;
        this.data = data;
    }

    public Tuple()
    {

    }
    public int getLength()
    {
        return data.length;
    }


    public  void setData(byte[] data)
    {
        this.data= data;
    }

    public byte[] getTupleByteArray() {
        return data;
    }

    public void setlength(int tuple_length) {
        this.length = tuple_length;
    }


}