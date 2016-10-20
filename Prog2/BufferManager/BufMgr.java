package bufmgr;

import global.*;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.nio.ByteBuffer;
import java.util.HashMap;

import chainexception.ChainException;

import diskmgr.DiskMgr;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;


public class BufMgr implements  GlobalConst
{
	private byte bufPool[][];
	private Descriptor bufDescr[]; 
	private HashMap<Integer,Integer> hashTable;
	private PolicyManager policyManager;
	
	private int descriptorCounter;
	private int numBufs;
	private String replaceArg;
	private boolean fullPool;
	
	/**
	* 1) Create the BufMgr object
	* 
	* 2) Allocate pages (frames) for the buffer pool in main memory and
	* 	 make the buffer manager aware that the replacement policy is
	* 	 specified by replaceArg (i.e. FIFO (Clock), LRU, MRU, love/hate)
	*
	* @param numBufs number of buffers in the buffer pool
	* @param replaceArg name of the buffer replacement policy
	* */
	public BufMgr(int numBufs,String replaceArg) 
	{
	    descriptorCounter = 0;
	    this.numBufs = numBufs;
	    this.replaceArg = replaceArg;
		bufPool = new byte[numBufs][GlobalConst.PAGE_SIZE];
		bufDescr = new Descriptor[numBufs]; // frame number to page number ; HOLDS PAGE NUMBER
		hashTable = new HashMap<Integer,Integer>(); // page number to frame number
		policyManager = new PolicyManager(replaceArg,numBufs);
		fullPool = false;
	}
	
	/**
	* Pin a page
	* First check if this page is already in the buffer pool.
	* If it is, increment the pin_count and return pointer to this page. 
	* If the pin_count was 0 before the call, the page was a
	* replacement candidate, but is no longer a candidate.
	* 
	* If the page is not in the pool, choose a frame (from the
	* set of replacement candidates) to hold this page, read the
	* page (using the appropriate method from diskmgr package) and pin it.
	* Also, must write out the old page in chosen frame if it is dirty
	* before reading new page. (You can assume that emptyPage == false for
	* this assignment.)
	*
	* @param pgid page number in the minibase.
	* @param page the pointer point to the page.
	* @param emptyPage true (empty page), false (nonempty page).
	*/
	public void pinPage(PageId pgid, Page page, boolean emptyPage) throws BufferPoolExceededException
	{
//			for (int name: hashTable.keySet()){
//
//				int key =name;
//				int value = hashTable.get(key);
//				System.out.println(key + " " + value);
//
//
//			}

		if(hashTable.containsKey(pgid.pid))
		{
			int frameNumber = hashTable.get(pgid.pid);
			int pinCount = bufDescr[frameNumber].getPinCount();
			
			if (pinCount == 0)	policyManager.removeAndShift(frameNumber);
		    
			bufDescr[frameNumber].incrementPinCount();
			// add to pool 
			page.setData(bufPool[frameNumber]);
			
		}
		// if not in the buffer pool and there is still empty place in the pool
		else if (descriptorCounter < numBufs) 
		{
			Page pg = new Page();
			try {	Minibase.DiskManager.read_page(pgid,pg);	}	// read the page
		    catch (InvalidPageNumberException | FileIOException | IOException e) 
			{	e.printStackTrace();}
			
			page.setData(pg.getpage());
			
		 // add to pool 
		    bufPool[descriptorCounter] = page.getpage();
		    
		    bufDescr[descriptorCounter] = new Descriptor(new PageId(pgid.pid),1,false);	 // add to descirptor 
            hashTable.put(pgid.pid,descriptorCounter);  // add to hashtable
            
		    descriptorCounter++;
		}
		// if replacement must be done or can't be done < FaYaSaT >
		else
		{
			// get a candidate frame and remove it from the policy
		  int frameNumber = policyManager.getReplacement(bufDescr,hashTable,pgid);
		  
		  if(frameNumber != -1) // there are replacement candidates
		  {
    		   // flush page
    		   if(bufDescr[frameNumber].isDirtyBit())  flushPage(bufDescr[frameNumber].getPgId());
    		   
    		    // remove old page from the hash map
    		   hashTable.remove(bufDescr[frameNumber].getPageNumber());
    		   
    		   Page pg = new Page();
    		   try {	Minibase.DiskManager.read_page(pgid,pg);	}	// read the page
               catch (InvalidPageNumberException | FileIOException | IOException e) 
               {	e.printStackTrace();}
    		   page.setData(pg.getpage());
    		   bufPool[frameNumber] = page.getpage(); // add to pool
    		   
    		   bufDescr[frameNumber].resetAll(new PageId(pgid.pid),1,false);	 // update to descirptor 
               hashTable.put(pgid.pid,frameNumber);  // add to hashtable
               
              
		   }
		    else
		    {
		    	fullPool = true;
		    	System.out.println("Memory FUll...");
		    	throw new BufferPoolExceededException(new Exception(),"bufmgr.BufferPoolExceededException");
		    }
		  
		}
	}

	/**
	* Unpin a page specified by a pageId.
	* This method should be called with dirty == true if the client has
	* modified the page. If so, this call should set the dirty bit
	* for this frame. Further, if pin_count > 0, this method should
	* decrement it. If pin_count = 0 before this call, throw an excpetion
	* to report error. (for testing purposes, we ask you to throw
	* an exception named PageUnpinnedExcpetion in case of error.)
	*
	* @param pgid page number in the minibase
	* @param dirty the dirty bit of the frame.
	*/
	public void unpinPage(PageId pgid, boolean dirty) throws HashEntryNotFoundException , PageUnpinnedException
	{
	    if(hashTable.containsKey(pgid.pid))
	    {
    	    int frameNumber = hashTable.get(pgid.pid);
    	    if(dirty)   bufDescr[frameNumber].setDirtyBit(dirty);
    	    if(bufDescr[frameNumber].getPinCount() > 0)
    	    {
    	        bufDescr[frameNumber].decrementPinCount();
                
    	        if(bufDescr[frameNumber].getPinCount() == 0)
    	        {
    	            policyManager.addReplacementCandidate(frameNumber);
    	            fullPool = false;
    	        }
    	    }
    	    else
    	    {
    	    	System.out.println("Pin_Count = 0 before Calling unpin method..");
    	    	throw new PageUnpinnedException(new Exception() , "bufmgr.PageUnpinnedExcpetion");
    	    }
	    }
	    // id not in buffer pool
	    else
	    {
	        System.out.println("Page is not in the pool.");
	        throw new HashEntryNotFoundException(new Exception(),"bufmgr.HashEntryNotFoundException");
	    }
	}
	
	/**
	* Allocate new page(s).
	* Call DB Object to allocate a run of new pages and
	* find a frame in the buffer pool for the first page
	* and pin it. (This call allows a client f the Buffer Manager
	* to allocate pages on disk.) If buffer is full, i.e., you
	* can\t find a frame for the first page, ask DB to deallocate
	* all these pages, and return null.
	*
	* @param firstPage the address of the first page.
	* @param howMany total number of allocated new pages.
	*
	* @return the first page id of the new pages. null, if error.
	*/
	public PageId newPage(Page firstPage, int howMany) throws ChainException
	{
		if(getNumUnpinned() != 0)
		{  
			PageId pgid = new PageId();
		    // allocate page
		    try {	Minibase.DiskManager.allocate_page(pgid,howMany);	}
		    catch (OutOfSpaceException | InvalidRunSizeException| InvalidPageNumberException | FileIOException| DiskMgrException | IOException e) 
			{	e.printStackTrace();	}
		    
		    pinPage(pgid,firstPage,false);
		    return pgid;
		}
		System.out.println("Memory Full.");
		return null;
	}

	/**
	* This method should be called to delete a page that is on disk.
	* This routine must call the method in diskmgr package to
	* deallocate the page.
	*
	* @param pgid the page number in the database.
	*/
	
	public void freePage(PageId pgid) throws PagePinnedException
	{
		
		if(hashTable.containsKey(pgid.pid))
	    {
	    	if(bufDescr[hashTable.get(pgid.pid)].getPinCount() <= 1 )
	    	{
	    		
	    		if(bufDescr[hashTable.get(pgid.pid)].getPinCount()!=0)
	    		{
	    			try {	unpinPage(pgid , false );	}
	    	    	catch (HashEntryNotFoundException | PageUnpinnedException e1) {e1.printStackTrace();}
	    		}
	    		
	    		bufDescr[hashTable.get(pgid.pid)].setDirtyBit(false);
	    		hashTable.remove(pgid.pid);
	    		
	    		
	    		try
				{
					Minibase.DiskManager.deallocate_page(pgid);

				}
		    	catch (ChainException e)
		    	{
					e.printStackTrace();

		    	}
	    	}
	    	else
	    	{
	    		System.out.println("Page is used by another user.");
	    		throw new PagePinnedException(new Exception(),"bufmgr.PagePinnedException");
	    	}
	    }
	    else
	    {
	        	try {	Minibase.DiskManager.deallocate_page(pgid);	}
				catch (ChainException e)
				{
					e.printStackTrace();

				}
		    	
	    }
		
	}

	/**
	* Used to flush a particular page of the buffer pool to disk.
	* This method calls the write_page method of the diskmgr package.
	*
	* @param pgid the page number in the database.
	*/
	public void flushPage(PageId pgid) 
	{
	    Page pg = new Page();
	    pg.setData(bufPool[hashTable.get(pgid.pid)]);
	    try {	Minibase.DiskManager.write_page(pgid,pg);}
	    catch (InvalidPageNumberException | FileIOException | IOException e) 
	    {	e.printStackTrace();	}
	    bufDescr[hashTable.get(pgid.pid)].setDirtyBit(false);
	}
	
	public void flushAllPages()
	{
		for(int i = 0 ; i < descriptorCounter ;i++)
		{
			if(bufDescr[i].isDirtyBit())
			{
				Page pg = new Page();
				pg.setData(bufPool[hashTable.get(bufDescr[i].getPageNumber())]);
				try {	Minibase.DiskManager.write_page(bufDescr[i].getPgId(),pg);	}
			    catch (InvalidPageNumberException | FileIOException | IOException e) 
			    {	e.printStackTrace();	}
				bufDescr[i].setDirtyBit(false);
			}
		}
	}

	public int getNumBufs()
	{
		return numBufs;
	}

	public int getNumUnpinned()
	{
        int number = numBufs-descriptorCounter;
        
        for(int i =0 ;i<descriptorCounter; i++)
        {
            if(bufDescr[i].getPinCount() == 0)    number++;
        }
        
		return number;
	}

}
