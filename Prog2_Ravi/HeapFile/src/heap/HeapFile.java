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

	private CapacityInfo capacInfo = new CapacityInfo();
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

				while (currentPageId.pid != -1 && currentPageId.pid!= 0)
				{
					HFPage temp1 = new HFPage();

					global.Minibase.BufferManager.pinPage(currentPageId, temp1,
							false);
					
					//System.out.println("Here" + currentPageId.pid);
					//recordNumber += amount(temp);
					global.Minibase.BufferManager.unpinPage(currentPageId, false);
					firstpgId.pid = firstpgId.pid+1;
					currentPageId = current.getNextPage();
					break;
				}

			}
		}
	}


	/*
	 * Given a capacity size return page id of live page in heapfile
	 * that satisifies this request.
	 */
	

	public RID insertRecord(byte[] record) throws ChainException
	{
		if (record.length > GlobalConst.MAX_TUPSIZE)
			throw new ChainException(new Exception(),"Record is too large");

		PageId pageId=null;
		HFPage hfPage= null;
		global.RID rid =null;
		
		pageId = capacInfo.getPageWithAvailCapacity((short)(record.length+4));
		if(pageId==null) {
			Page page = new Page(); // else create a new page
			pageId = global.Minibase.BufferManager.newPage(page, 1);
			hfPage = new HFPage(page); // copy that page as a hfpage
			hfPage.initDefaults();
			hfPage.setCurPage(pageId); // set the page id for that hfpage
			hfPage.setData(page.getData());
			rid = hfPage.insertRecord(record);
			capacInfo.insert((Short)hfPage.getFreeSpace(),pageId);
			current.setNextPage(pageId);
			hfPage.setPrevPage(current.getCurPage());
			current= hfPage;
		}
		else
		{
			Page page = new Page();
			global.Minibase.BufferManager.pinPage(pageId, page, false);
			hfPage = new HFPage(page);	
			hfPage.setCurPage(pageId);
			hfPage.setData(page.getData());
			rid = hfPage.insertRecord(record);	
			capacInfo.insert(hfPage.getFreeSpace(), pageId);
			current.setNextPage(pageId);
			hfPage.setPrevPage(current.getCurPage());
			current = hfPage;
			
		}
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
		
		if(!capacInfo.containsPageId(pageid)) return null;
		
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
		if(!capacInfo.containsKeyAndPageId(hfpage.getFreeSpace(), pageid)) return false;
		capacInfo.removePageId(hfpage.getFreeSpace(), pageid);
		hfpage.updateRecord(rid,newRecord); // delete the rid in hfpage
		capacInfo.insert((short)(hfpage.getFreeSpace()),pageid);

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

		if(!capacInfo.containsPageId(pageid)) return false;
	
		capacInfo.removePageId(hfpage.getFreeSpace(), pageid);

		hfpage.deleteRecord(rid); // delete the rid in hfpage
		capacInfo.insert((short)(hfpage.getFreeSpace()- rid.getLength()),pageid);

		global.Minibase.BufferManager.unpinPage(pageid, true); // modified, so unpin it with dirty bit
		recordNumber--; // record number reduced
		return true;
	}
	public int getRecCnt()  //get number of records in the file
	{
		return recordNumber;
	}

	public Iterator<PageId> iterator()
	{
		return capacInfo.iterator();
	}

	public HeapScan openScan()
	{
		return new HeapScan(this);
	}

}