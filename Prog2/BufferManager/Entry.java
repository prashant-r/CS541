package bufmgr;

public class Entry 
{
    public static long numberGenerator = -1;
    private int frameNumber;
    private long number;
    
	public Entry(int frameNumber)
	{
	    this.frameNumber = frameNumber;
        number = ++numberGenerator;	
	}
	@Override
	public String toString() {
	
		return "Number: "+number+" ,Frame: "+frameNumber +" ,Loved: ";
	}

	public int getFrameNumber() {
		return frameNumber;
	}

	public long getNumber() {
		return number;
	}
	
}
    
	
