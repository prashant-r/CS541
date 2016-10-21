package bufmgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import chainexception.ChainException;
import global.GlobalConst;
import global.Page;
import global.PageId;

public class BufFrmDescriptor implements Comparable<BufFrmDescriptor> {
	
//-----------------------------------------------------------------------------------------------------------
	// Class declarations and access methods
//---------------------------------------------------------------------------------------------------------------	

	private static MyHashTable pageId_frameId_lookup = new MyHashTable();
	
	public static Integer getFrameIDForPageID(Integer pid) throws ChainException
	{
		if(!pageId_frameId_lookup.containsKey(pid))
		{
			throw new HashEntryNotFoundException(new Exception(),"ERROR MSG : Hash entry not found exception.");
		}
		return pageId_frameId_lookup.get(pid);
	}
	
	
	public static void removeFrameIDForPageID(Integer pid)
	{
		pageId_frameId_lookup.remove(pid);
		return;
	}	
	
		
//---------------------------------------------------------------------------------------------------------------	
	// Object declarations and access methods
//---------------------------------------------------------------------------------------------------------------	
	
	// TODO: Change refer to system time
	List<Integer> referenceTimes;
	int pin_count;
	byte[] frame_data;
	Integer fid;
	PageId page_number;
	boolean dirty;
	
	public BufFrmDescriptor(int fid) {
		super();
		this.pin_count = 0;
		this.frame_data = new byte[GlobalConst.PAGE_SIZE];
		this.fid = fid;
		this.dirty = false;
		referenceTimes = new ArrayList<Integer>();
		page_number = null;
	}
	
	public void resetFrame()
	{
		if(page_number != null)	
			if(pageId_frameId_lookup.get(page_number.pid).equals(fid))
				pageId_frameId_lookup.remove(page_number.pid);
		this.pin_count = 0;
		this.frame_data = new byte[GlobalConst.PAGE_SIZE];
		this.dirty = false;
		referenceTimes = new ArrayList<Integer>();
		
	}
	
	public void insertIntoFrame(Page page, PageId pageId)
	{
		// Insert into lookup tables
		
		//System.out.println("Insert into hash " + pageId.pid  + " fid is "+ this.fid);
		
		pageId_frameId_lookup.put(pageId.pid, this.fid);
		
		// Update local vars
		this.page_number = pageId;
		
		// Update the data frame
		this.frame_data = Arrays.copyOf(page.getData(), page.getData().length);
		
		// Update the crf time
		this.referenceTimes.clear();
		this.referenceTimes.add(BufMgr.ctime);
		this.dirty = false;
		this.pin_count = 1;
	}
	
	@Override
	public String toString() {
		return "BufFrmDescriptor [crf = " +  crf(BufMgr.ctime) + ", pin_count=" + pin_count +  ", fid=" + fid + ", page_number=" + page_number + ", dirty=" + dirty
				+ "]";
	}


	public double crf(int currentTime)
	{
		double crfval =0.0;
		for(int a =0 ; a < referenceTimes.size(); a++)
		{
			crfval = crfval + (double)(1.0d/ ( X(referenceTimes.get(a), currentTime) + 1));
		}
		return crfval;
	}
	
	private int X(int referenceTime, int currentTime)
	{
		return currentTime - referenceTime;
	}

	@Override
	public int compareTo(BufFrmDescriptor o) {
		Double crfA = new Double(this.crf(BufMgr.ctime));
		Double crfB = new Double(o.crf(BufMgr.ctime));
		return crfA.compareTo(crfB);
	}
	
	
}
