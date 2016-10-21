package heap;

import java.util.List;

import org.jgrapht.util.*;

import global.Minibase;
import global.Page;
import global.PageId;

public class HeapFile<T> {
	
	List<PageId> pages;
	
	public HeapFile(String name)
	{
		if(name==null)
		{
			Page page = new Page();
			PageId pgId = Minibase.BufferManager.newPage(page, 1);
			HFPage hp = new HFPage(page);
			pages.add(pgId);
			Minibase.BufferManager.unpinPage(pgId, false);
			
			
			
		
		}
	}
	public RID insertRecord(byte[] record)
	{
		return null;
	}
	public Tuple getRecord(RID rid)
	{
		return null;
	}
	public boolean updateRecord(RID rid, Tuple newRecord)
	{
		return false;
	}
	public boolean deleteRecord(RID rid)
	{
		return false;
	}
	public int getRecCnt() //get number of records in the file
	{
		return 0;
	}
	public HeapScan openScan()
	{
		return null;
	}

}
