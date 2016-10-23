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
    private Iterator<LinkedList<PageId>> it;
    private LinkedList<PageId> currentPageList;
    Iterator<PageId> listiterator ;

    protected HeapScan(HeapFile hf)
    {
        this.hf = hf;
        it = hf.iterator();
        currentPageList = it!=null ? it.next() : null;

        currentPageId= (currentPageList==null) || (currentPageList.size()==0) ? null : currentPageList.getFirst();

        if(currentPageId==null) return;

        listiterator = currentPageList.iterator();

        Page page=new Page();
        Minibase.BufferManager.pinPage(currentPageId, page, false);

        currentPage = new HFPage(page);
        currentRid = currentPage.firstRecord();
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
            while (hasNext())
            {
                   if(currentPageList.size()!=0)
                   {
                       Minibase.BufferManager.unpinPage(currentPage.getCurPage(), false);
                       currentPageList.poll();

                   }
                   else
                       currentPageList = it.next() ;

                    currentPageId= (currentPageList==null) || (currentPageList.size()==0) ? null : currentPageList.getFirst();

                   if(currentPageId==null)
                   {
                      continue;
                   }

                   Page page=new Page();
                   Minibase.BufferManager.pinPage(currentPageId, page, false);

                   currentPage = new HFPage(page);
                   currentRid = currentPage.firstRecord();
                   break;

            }
        }

        if(currentRid!=null)
        {
            rid.copyRID(currentRid);
            currentRid = currentPage.nextRecord(currentRid);
            return new Tuple(currentPage.selectRecord(rid));

        }
        return null;
    }

}