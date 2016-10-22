package heap;

import java.util.*;

import global.Minibase;
import global.Page;
import global.PageId;


import chainexception.ChainException;

public class HeapFile {
	
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

	TreeMap<Integer, LinkedList<PageId>> capacityPageQueue = new TreeMap<Integer, LinkedList<PageId>>();


	public PageId getFirstAvailableCapacity(int cap)
	{

		List<Integer> possibleCapacities = new ArrayList<Integer>(capacityPageQueue.keySet());
		binarySearchUB(possibleCapacities, cap);

		return null;
	}


	public Integer binarySearchUB(List<Integer> searchSet, Integer toFind)
	{
		int low = 0;
		int high = searchSet.size()-1;
		int mid = low;
		while(low < high)
		{
			mid = low + (high - low )/2;
			int res = searchSet.get(mid).compareTo(toFind);
			if(res >0) high = mid;
			low = mid+1;
		}
		if (searchSet.get(mid).compareTo(toFind) != 0) high--;
		return high;
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
