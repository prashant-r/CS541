package heap;

import java.util.*;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import global.RID;


import chainexception.ChainException;

public class HeapFile {
	
	//---------------------------------------------------------------------------------------------------------------	
		// Object declarations and access methods
	//---------------------------------------------------------------------------------------------------------------	

	private TreeMap<Short, LinkedList<PageId>> capInfo = new TreeMap<Short, LinkedList<PageId>>();

	int recordNumber=0;
	private PageId firstpgId;
	private HFPage current;
	
	public HeapFile(String name)
	{

		if(name==null)
		{
			// create a temporary heap file which creates no DB entry
			Page page = new Page();
			firstpgId = Minibase.BufferManager.newPage(page, 1);
			current = new HFPage(page);
			current.setCurPage(firstpgId);

			LinkedList<PageId> pageIdList = new LinkedList<PageId>();
			pageIdList.add(firstpgId);
			Minibase.BufferManager.unpinPage(firstpgId, true);
			recordNumber=0;
		}
		else{
			// try to open the file
			firstpgId= Minibase.DiskManager.get_file_entry(name);

			if(firstpgId == null)
			{
				// file name not found
				Page page = new Page();
				firstpgId = Minibase.BufferManager.newPage(page, 1);
				Minibase.DiskManager.add_file_entry(name, firstpgId);
				recordNumber=0;

				current = new HFPage(page);
				current.setCurPage(firstpgId);

				Minibase.BufferManager.unpinPage(firstpgId, true);
				
			}
			else
			{
				Page page = new Page();
				global.Minibase.BufferManager.pinPage(firstpgId, page, false);  // if found; pin it ; accessing


				current =new HFPage(page);  //// create a new HFPage with the page created before, make it as current Page
				current.setData(page.getData());


				//recordNumber += amount(current);  //  increment the record number depending on the current page
				global.Minibase.BufferManager.unpinPage(firstpgId, false);  // unpin it, done accessing
				PageId currentPageId = current.getNextPage();
				//LinkedList<PageId> pageIdList = new LinkedList<PageId>();

//				while (currentPageId.pid != 0 && currentPageId.pid != -1)
//				{
//					HFPage temp1 = new HFPage();
//
//					global.Minibase.BufferManager.pinPage(currentPageId, temp1,
//							false);
//
//					if(capInfo.containsKey(current.getFreeSpace()))
//					{
//						LinkedList<PageId> newpageIdList = capInfo.get(current.getFreeSpace());
//						newpageIdList.add(currentPageId);
//						capInfo.replace(current.getFreeSpace(), newpageIdList);
//					}
//					else
//					{
//						pageIdList = new LinkedList<PageId>();
//						pageIdList.add(currentPageId);
//						capInfo.put(current.getFreeSpace(),pageIdList);
//					}
//
//					//recordNumber += amount(temp);
//					global.Minibase.BufferManager.unpinPage(currentPageId, false);
//					firstpgId.pid = firstpgId.pid+1;
//					currentPageId = current.getNextPage();
//				}

			}
		}
	}


	/*
	 * Given a capacity size return page id of live page in heapfile
	 * that satisifies this request.
	 */
	public PageId getPageWithAvailCapacity(int cap) throws Exception
	{
		// Note : can also use tree map function - ceilingEntry for same task.
		LinkedList<PageId> ll = capInfo.ceilingEntry((short)cap).getValue();

		if(ll.isEmpty()) throw new Exception("Poll attempted on empty LinkedList. "); 
		return ll.getFirst();
	}

	public RID insertRecord(byte[] record) throws ChainException
	{
		if (record.length > GlobalConst.MAX_TUPSIZE)
			throw new ChainException(new Exception(),"Record is too large");

		PageId pageId=null;
		HFPage hfPage= null;
		PageId newPageId=null;

		boolean newPageAdded= false;

		try
		{
			if(!capInfo.isEmpty())
			pageId = getPageWithAvailCapacity(record.length+4);
		}
		catch (Exception e) {

		}

		if(pageId==null) {

			Page page = new Page(); // else create a new page
			pageId = global.Minibase.BufferManager.newPage(page, 1);

			hfPage = new HFPage(page); // copy that page as a hfpage
			hfPage.initDefaults();
			hfPage.setCurPage(pageId); // set the page id for that hfpage

			hfPage.setData(page.getData());
			newPageAdded=true;
			newPageId= pageId;
		}
		else
		{
			Page page = new Page();
			global.Minibase.BufferManager.pinPage(pageId, page, false);
			hfPage = new HFPage(page);
			hfPage.setCurPage(pageId);
			hfPage.setData(page.getData());


		}

		global.RID rid =null;

		if(capInfo.isEmpty() || newPageAdded)
		{
			rid = hfPage.insertRecord(record); // insert the record
			LinkedList<PageId> pageIdList = new LinkedList<PageId>();
			pageIdList.add(pageId);
			capInfo.put((Short)hfPage.getFreeSpace(),pageIdList);
			newPageAdded= false;

			hfPage.setNextPage(pageId);
			hfPage.setPrevPage(current.getCurPage());
			current= hfPage;
		}
		else
		{
			short freespace = hfPage.getFreeSpace();
			LinkedList<PageId> pageIdList = capInfo.get(hfPage.getFreeSpace());

			rid = hfPage.insertRecord(record);

			if(capInfo.containsKey(hfPage.getFreeSpace()))
			{
				LinkedList<PageId> previousList = capInfo.get(hfPage.getFreeSpace());
				previousList.addAll(pageIdList);
				capInfo.replace(hfPage.getFreeSpace(),previousList);
				capInfo.remove(freespace);
			}
			else {
				capInfo.remove(freespace);
				capInfo.put(hfPage.getFreeSpace(), pageIdList);
			}

		}

		//hfPage.setPrevPage(current.getCurPage());
		//current = hfpage;
		recordNumber++; // keep track of the record number
		Minibase.BufferManager.unpinPage(pageId,true);

		return rid;

	}
	public Tuple getRecord(RID rid) throws ChainException
	{
		PageId pageid =  rid.pageno;
		if(pageid== null ) return null;

		Page page = new Page();
		global.Minibase.BufferManager.pinPage(pageid, page, false); // pin that particular page with pid
		HFPage hfpage = new HFPage(page);

		LinkedList<PageId> linkedList=  capInfo.get(hfpage.getFreeSpace());

		if(linkedList.isEmpty() || !linkedList.contains(pageid)) return null;

		Tuple tuple= new Tuple();

		tuple.setData(hfpage.selectRecord(rid));
		global.Minibase.BufferManager.unpinPage(pageid, false);
		return tuple;
	}

	public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException
	{
		PageId pageid =  rid.pageno;
		if(pageid== null ) return false;

		if(rid.getLength() != newRecord.getLength()) return  false;

		Page page = new Page();
		global.Minibase.BufferManager.pinPage(pageid, page, false); // pin that particular page with pid
		HFPage hfpage = new HFPage(page);

		LinkedList<PageId> linkedList=  capInfo.get(hfpage.getFreeSpace());

		if(linkedList.isEmpty() || !linkedList.contains(pageid)) return false;

		capInfo.remove(hfpage.getFreeSpace());

		hfpage.updateRecord(rid,newRecord); // delete the rid in hfpage
		capInfo.put((short)(hfpage.getFreeSpace()),linkedList);

		global.Minibase.BufferManager.unpinPage(pageid, true); // modified, so unpin it with dirty bit
		return true;
	}
	public boolean deleteRecord(RID rid)
	{

		PageId pageid =  rid.pageno;
		if(pageid== null ) return false;

		Page page = new Page();
		global.Minibase.BufferManager.pinPage(pageid, page, false); // pin that particular page with pid
		HFPage hfpage = new HFPage(page);

		LinkedList<PageId> linkedList=  capInfo.get(hfpage.getFreeSpace());

		if(linkedList.isEmpty() || !linkedList.contains(pageid)) return false;

		capInfo.remove(hfpage.getFreeSpace());

		hfpage.deleteRecord(rid); // delete the rid in hfpage
		capInfo.put((short)(hfpage.getFreeSpace()- rid.getLength()),linkedList);

		global.Minibase.BufferManager.unpinPage(pageid, true); // modified, so unpin it with dirty bit
		recordNumber--; // record number reduced
		return true;
	}
	public int getRecCnt()  //get number of records in the file
	{
		return recordNumber;
	}

	public Iterator<LinkedList<PageId>> iterator()
	{

		if(capInfo.isEmpty()) return  null;

		return capInfo.values().iterator();

	}

	public HeapScan openScan()
	{
		return new HeapScan(this);
	}

}