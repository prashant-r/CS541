package heap;

import chainexception.ChainException;
import global.Minibase;
import global.Page;
import global.PageId;
import global.RID;

import java.util.Iterator;
import java.util.LinkedList;

public class HeapScan {

    private HeapFile hf;
    private PageId currentPageId;
    private HFPage currentPage;

    private RID currentRid;
    Iterator<PageId> it ;

    protected HeapScan(HeapFile hf)
    {
        this.hf = hf;
        it = hf.iterator();
        Page page=new Page();
        if(it.hasNext())
        {
        	currentPageId = it.next();
        	Minibase.BufferManager.pinPage(currentPageId, page, false);
            currentPage = new HFPage(page);
            currentRid = currentPage.firstRecord();

        }
    }

    protected void finalize() throws Throwable
    {
        currentPage = null;
        currentRid=null;
        it = null;
    }

    public void close() throws ChainException
    {
        try
        {
            finalize();
        }
        catch (Throwable e)
        {

            e.printStackTrace();
        }

        hf = null;
    }

    public boolean hasNext()
    {
        if(this.it==null) return false;
        return it.hasNext();
    }

    public Tuple getNext(RID rid)
    {
        if(!hasNext() && currentRid==null && currentPage!=null)
            Minibase.BufferManager.unpinPage(currentPage.getCurPage(), false);
        if (currentRid == null)
        {
            while (it.hasNext())
            {
                   Page page=new Page();
                   currentPageId = it.next();
                   Minibase.BufferManager.pinPage(currentPageId, page, false);
                   currentPage = new HFPage(page);
                   currentRid = currentPage.firstRecord();
            }
        }
        else
        {
            rid.copyRID(currentRid);
            currentRid = currentPage.nextRecord(currentRid);
            return new Tuple(currentPage.selectRecord(rid));

        }
        return null;
    }

}