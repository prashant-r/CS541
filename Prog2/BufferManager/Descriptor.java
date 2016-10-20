package bufmgr;

import global.PageId;

public class Descriptor 
{
	private int pageNumber;
	private PageId pgId;
	private int pinCount;
	private boolean dirtyBit;
	
	public Descriptor(PageId pgId,int pinCount ,boolean dirtyBit)
	{
		this.pgId = pgId;
		this.pageNumber = pgId.pid;
		this.pinCount = pinCount;
		this.dirtyBit = dirtyBit;
	}
	
	public void resetAll(PageId pgId,int pinCount ,boolean dirtyBit)
	{
		this.pgId = pgId;
		this.pageNumber = pgId.pid;
		this.pinCount = pinCount;
		this.dirtyBit = dirtyBit;
	}
	
	public void incrementPinCount()
	{
		this.pinCount+=1;
	}
	public void decrementPinCount()
	{
		this.pinCount-=1;;
	}

	public int getPinCount() 
	{
		return pinCount;
	}

	public PageId getPgId() {
		return pgId;
	}

	public int getPageNumber() 
	{
		return pageNumber;
	}

	public boolean isDirtyBit() 
	{
		return dirtyBit;
	}

	public void setDirtyBit(boolean dirtyBit) 
	{
		this.dirtyBit = dirtyBit;
	}
	@Override
	public String toString() 
	{
		return "pageNumber: "+pageNumber+" ,pinCount: "+pinCount+" ,dirtyBit: "+dirtyBit;
	}

	
}
