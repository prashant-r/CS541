package heap;

import java.util.*;

import global.Minibase;
import global.Page;
import global.PageId;
import chainexception.ChainException;

public class HeapFile {
	
	//---------------------------------------------------------------------------------------------------------------	
		// Object declarations and access methods
	//---------------------------------------------------------------------------------------------------------------	
		
	
	List<PageId> pages;
	
	public HeapFile(String name)
	{
		if(name==null)
		{
			// create a temporary heap file which creates no DB entry
			Page page = new Page();
			PageId pgId = Minibase.BufferManager.newPage(page, 1);
			HFPage hp = new HFPage(page);
			pages.add(pgId);
			Minibase.BufferManager.unpinPage(pgId, false);
		}
		else{
			// try to open the file
			if(Minibase.DiskManager.get_file_entry(name) == null)
			{
				// file name not found
				Page page = new Page();
				PageId pgId = Minibase.BufferManager.newPage(page, 1);
				Minibase.DiskManager.add_file_entry(name, pgId);
				
			}
			else
			{
				// file is found
			}
		}
	}

	private TreeMap<Integer, LinkedList<PageId>> capInfo = new TreeMap<Integer, LinkedList<PageId>>();

	/*
	 * Given a capacity size return page id of live page in heapfile
	 * that satisifies this request.
	 */
	public PageId getPageWithAvailCapacity(int cap) throws Exception
	{
		// Note : can also use tree map function - ceilingEntry for same task.
		Map.Entry<Integer, LinkedList<PageId>> entry= capInfo.ceilingEntry(cap);
		if(entry == null) return null;
		LinkedList<PageId> pageOptions = entry.getValue();
		if(pageOptions.isEmpty()) throw new Exception("Poll attempted on empty LinkedList. "); 
		return pageOptions.poll();
	}

	public RID insertRecord(byte[] record) throws ChainException
	{
		return null;
	}
	public Tuple getRecord(RID rid) throws ChainException
	{
		return null;
	}
	public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException
	{
		return false;
	}
	public boolean deleteRecord(RID rid) throws ChainException
	{
		return false;
	}
	public int getRecCnt()  //get number of records in the file
	{
		return 0;
	}
	public HeapScan openScan()
	{
		return new HeapScan(this);
	}

}
