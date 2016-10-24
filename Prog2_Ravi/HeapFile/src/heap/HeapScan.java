package heap;

import chainexception.ChainException;
import global.PageId;
import global.RID;

import java.util.Iterator;
import java.util.LinkedHashMap;


public class HeapScan {
	
	private PageId firstPageId;
	private Integer currentPid;
	private RID currentRid;
	private Iterator<Integer> it;
	private LinkedHashMap<Integer, HFPage> hfPagesMap;

	protected HeapScan(HeapFile hf) {
		hfPagesMap = hf.getAllHFPages();
		//System.out.println(hfPagesMap);
		HFPage firstPage = new HFPage();
		firstPageId = hf.firstpgId;
		global.Minibase.BufferManager.pinPage(firstPageId, firstPage, false); // pin that particular page with pid
		it = pageIdIterator();
		currentPid = it.next();
		//System.out.println(currentPid);
		setFirstRid();
	}
	
	private void setFirstRid()
	{
		if(currentPid == null)
		{		
			currentRid = null;
			return;
		}
		RID tempRID = hfPagesMap.get(currentPid).firstRecord();
		if(tempRID == null)
		{
			currentPid = nextPid();
			setFirstRid();
		}
		else
		{
			currentRid = new RID();
			currentRid.copyRID(tempRID);
		}
	}
	
	
	private Iterator<Integer> pageIdIterator()
	{
		return hfPagesMap.keySet().iterator();
	}

	protected void finalize() throws Throwable {
		currentPid = null;
		currentRid = null;
	}

	public void close() throws ChainException {

		try {
			finalize();
		} catch (Throwable e) {
			throw new ChainException(null, e.getMessage());
		}
	}

	public boolean hasNext() {
		return currentRid != null;
	}
		
	private Integer nextPid()
	{
		if(it.hasNext()) return it.next();
		return null;
	}

	public Tuple getNext(RID rid) {
		
		if(currentRid == null) {
			global.Minibase.BufferManager.unpinPage(firstPageId, true); // modified, so unpin it with dirty bit
			return null;
		}
		HFPage currentPage = hfPagesMap.get(currentPid);
		rid.copyRID(currentRid);
		Tuple toReturn = new Tuple(currentPage.selectRecord(currentRid));
		//System.out.println(toReturn);
		RID nextRID = currentPage.nextRecord(currentRid);
		if(nextRID == null)
		{
			currentPid = nextPid();
			setFirstRid();
		}
		else
		{
			currentRid = new RID();
			currentRid.copyRID(nextRID);
		}
		return toReturn;
	}
}