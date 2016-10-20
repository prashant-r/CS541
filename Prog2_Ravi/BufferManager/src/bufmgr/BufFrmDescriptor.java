package bufmgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import global.GlobalConst;
import global.Page;
import global.PageId;

public class BufFrmDescriptor {
	
//-----------------------------------------------------------------------------------------------------------
	// Class declarations and access methods
//---------------------------------------------------------------------------------------------------------------	

	private static MyHashTable pageId_frameId_lookup = new MyHashTable();
	
	public static Integer getFrameIDForPageId(Integer pid)
	{
		if(!pageId_frameId_lookup.containsKey(pid)) return null;
		return pageId_frameId_lookup.get(pid);
	}
		
//---------------------------------------------------------------------------------------------------------------	
	// Object declarations and access methods
//---------------------------------------------------------------------------------------------------------------	
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
	}
	
	public void insertIntoFrame(Page page, PageId pageId)
	{
		// Insert into lookup tables
		pageId_frameId_lookup.put(pageId.pid, this.fid);
		
		// Update local vars
		this.page_number = pageId;
		
		// Update the data frame
		this.frame_data = Arrays.copyOf(page.getData(), page.getData().length);
	}
	
	
	
}