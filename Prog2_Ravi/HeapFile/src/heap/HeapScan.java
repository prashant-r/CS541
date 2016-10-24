package heap;

import chainexception.ChainException;
import global.Minibase;
import global.Page;
import global.PageId;
import global.RID;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class HeapScan {

    private HeapFile hf;
    private PageId currentPageId;
    private HFPage currentPage;
    private RID currentRid;
    private LinkedHashMap<Integer, HFPage> hfpagesMap;

    protected HeapScan(HeapFile hf)
    {
        this.hf = hf;
        currentPageId = hf.firstpgId;
        hfpagesMap = hf.getAllHFPages();   
    }

    protected void finalize() throws Throwable
    {
    	currentPageId = null;
        currentPage = null;
        currentRid=null;
    }

    public void close() throws ChainException
    {

        try {
			finalize();
		} catch (Throwable e) {
			throw new ChainException(null, e.getMessage());
		}
    }

    public boolean hasNext()
    {
    	return false;
    }

    public Tuple getNext(RID rid)
    {   
    	//Minibase.BufferManager.pinPage(currentPageId, page, false);
        //currentPage = new HFPage(page);
    	return null;
    }

}