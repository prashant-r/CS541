package bufmgr;

import global.Page;
import global.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import chainexception.ChainException;
import global.Minibase;

public class BufMgr {
	public BufFrmDescriptor[] bufDescr;

	// TODO: change to system time
	public static int ctime = 0;

	/**
	 * Create the BufMgr object. Allocate pages (frames) for the buffer pool in
	 * main memory and make the buffer manage aware that the replacement policy
	 * is specified by replacerArg (e.g., LH, Clock, LRU, MRU, LRFU, etc.).
	 *
	 * @param numbufs
	 *            number of buffers in the buffer pool
	 * @param lookAheadSize
	 *            number of pages to be looked ahead, you can ignore that
	 *            parameter
	 * @param replacementPolicy
	 *            Name of the replacement policy, that parameter will be set to
	 *            "LRFU"
	 */

	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
		// Allocate the pages (frames) for the buffer pool in main memory
		bufDescr = new BufFrmDescriptor[numbufs];
		for (int a = 0; a < numbufs; a++)
			bufDescr[a] = new BufFrmDescriptor(a);
		BufFrmDescriptor.resetPageId_frameId_lookup();
	};

	/**
	 * Pin a page. First check if this page is already in the buffer pool. If it
	 * is, increment the pin_count and return a pointer to this page. If the
	 * pin_count was 0 before the call, the page was a replacement candidate,
	 * but is no longer a candidate. If the page is not in the pool, choose a
	 * frame (from the set of replacement candidates) to hold this page, read
	 * the page (using the appropriate method from diskmgr package) and pin it.
	 * Also, must write out the old page in chosen frame if it is dirty before
	 * reading new page.__ (You can assume that emptyPage==false for this
	 * assignment.)
	 *
	 * @param pageno
	 *            page number in the Minibase.
	 * @param page
	 *            the pointer pointing to the page.
	 * @param emptyPage
	 *            true (empty page); false (non-empty page)
	 */
	public void pinPage(PageId pageno, Page page, boolean emptyPage) throws ChainException {

		// if (pageno.pid == 0)
		// System.out.println("PINNING PAGE Page number " + pageno.pid);
		//System.out.println("CALLLEEED PIN PAGE WITH PAGE id " + pageno.pid );
		// Check if this page is already in the buffer pool.
		Integer fr_id = null;
		try {
			fr_id = BufFrmDescriptor.getFrameIDForPageID(pageno.pid);
		} catch (HashEntryNotFoundException e) {

		}
		// if (pageno.pid == 0 && fr_id != null)
		// System.out.println("THE FRAME PIN COUNT IS " +
		// bufDescr[fr_id].pin_count);

		// Increment the time
		//System.out.println("THE FRAMeeee id chosen is "  + fr_id);
		if(fr_id != null){
		//System.out.println("Cool BEFFFOORE we are done " + bufDescr[fr_id]);
		}
		else
		{
		//	System.out.println("Frame id was null");
		}
		ctime++;

		// If it is, increment the pin_count
		if (fr_id != null) {
			BufFrmDescriptor bfd = bufDescr[fr_id];
			assert bfd.getPage_number().pid == pageno.pid;
			//System.out.println("Cool BEFFFOORE we are done " + bfd);
			//System.out.println("Made it here ");
			// if the pin count was 0 before the call
			if (bfd.getPin_count()== 0) {
				bfd.getReferenceTimes().clear();
			}
			bfd.setPin_count(bfd.getPin_count() +1);
			bfd.getReferenceTimes().add(ctime);
			page.setpage(bfd.getFrame_data());
			//System.out.println("Cool AFFFTERR we are done " + bfd);
		} else {

			// It is not in the pool, choose a frame to replace.
			List<BufFrmDescriptor> bufsToConsider = new ArrayList<BufFrmDescriptor>();
			
			for(int a =0 ; a <  getNumBuffers() ; a++)
			{
				if(bufDescr[a].getPin_count() == 0)
					bufsToConsider.add(bufDescr[a]);
			}
			if(bufsToConsider.isEmpty())
				throw new BufferPoolExceededException(new Exception(),
					"ERROR MSG : memory is no unpinned page frams available.");
			
			BufFrmDescriptor toEvict = Collections.min(bufsToConsider);

//			System.out.println("*******************");
//			System.out.println("DOING FOR" );
//			System.out.println(pageno.pid);
//			System.out.println("--------------------------THE ONE CHOSEN ------------------");
//			System.out.println(toEvict);
//			System.out.println("-----------------------------------------------------------");
//			// if (toEvict.page_number != null && toEvict.page_number.pid == 0)
			// {
			// System.out.println("THIS CHANGED ");
			// }
			//
			try {
				Minibase.DiskManager.read_page(pageno, page);
			} catch (Exception e) {
				throw new ChainException(e, "ERROR MSG: couldn't read from disk. ");
			}

			if (toEvict.getPage_number() != null) {
				if (toEvict.isDirty())
					flushPage(toEvict.getPage_number());
				BufFrmDescriptor.removeFrameIDForPageID(toEvict.getPage_number().pid);
			}
			
			toEvict.resetFrame();
			toEvict.insertIntoFrame(page, pageno);
//			System.out.println("GAARLIC ACHAR FTW");
//			System.out.println("Cool we are done " + toEvict);
//			System.out.println("YEEHEHAHAAH");
		}

	};

	/**
	 * Unpin a page specified by a pageId. This method should be called with
	 * dirty==true if the client has modified the page. If so, this call should
	 * set the dirty bit for this frame. Further, if pin_count>0, this method
	 * should decrement it. If pin_count=0 before this call, throw an exception
	 * to report error. (For testing purposes, we ask you to throw an exception
	 * named PageUnpinnedException in case of error.)
	 *
	 * @param pageno
	 *            page number in the Minibase.
	 * @param dirty
	 *            the dirty bit of the frame
	 */
	public void unpinPage(PageId pageno, boolean dirty) throws ChainException {

		//System.out.println("CALLLEEED UNPIN PAGE with pageno " + pageno.pid );
		
		Integer fr_id = null;
		try {
			fr_id = BufFrmDescriptor.getFrameIDForPageID(pageno.pid);
		} catch (HashEntryNotFoundException e) {
			throw e;
		}
		BufFrmDescriptor bufd = bufDescr[fr_id];
		assert bufd.getPage_number().pid == pageno.pid;
		//System.out.println("Cool we are done NOW 2 \n " + bufDescr[1]);
		// if (pageno.pid == 0)
		// System.out.println("UNPINNING PAGE NO. " + pageno.pid);
		// if (pageno.pid == 0)
		// System.out.println("THE FRAME PIN COUNT IS HEERE " +
		// bufDescr[fr_id].pin_count);
		if (bufd.getPin_count() > 0) {
			if (dirty)
				bufd.setDirty(true);
			bufd.setPin_count(bufd.getPin_count() -1);
			// if (pageno.pid == 0)
			// System.out.println("THE FRAME PIN COUNT IS HEERERRR " +
			// bufDescr[fr_id].pin_count);
		} else {

			throw new PageUnpinnedException(new Exception(), " ERROR MSG : page was unpinned.");
		}

	};

	/**
	 * Allocate new pages. Call DB object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page and pin it. (This call
	 * allows a client of the Buffer Manager to allocate pages on disk.) If
	 * buffer is full, i.e., you can't find a frame for the first page, ask DB
	 * to deallocate all these pages, and return null.
	 *
	 * @param firstpage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 *
	 * @return the first page id of the new pages.__ null, if error.
	 */
	public PageId newPage(Page firstpage, int howmany) throws ChainException {
		
//		System.out.println("CALLLEEED NEW PAGE " );
//		System.out.println("LOOOOKKKKKKKKKKKKKKKKKKK AT TTHTHTISSSSS SHSIT \n " + bufDescr[1]);
		PageId pgId = new PageId();
//		System.out.println(pgId.pid);
		try {
			Minibase.DiskManager.allocate_page(pgId, howmany);
		} catch (Exception e) {
			throw new ChainException(e, "ERROR MSG : allocate page request to disk failed.");
		}
		try {
			pinPage(pgId, firstpage, global.GlobalConst.PIN_MEMCPY);
		} catch (PagePinnedException e) {
			try {
				Minibase.DiskManager.deallocate_page(pgId, howmany);
			} catch (Exception eprime) {
				throw new ChainException(eprime, "ERROR MSG: deallocate several pages request in disk failed.");
			}
			return null;
		} catch (Exception e) {
			throw e;
		}
		return pgId;
	};

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 *
	 * @param globalPageId
	 *            the page number in the data base.
	 */
	public void freePage(PageId globalPageId) throws ChainException {
		//System.out.println("CALLLEEED FREEE PAGE " );
		Integer fr_id = null;
		try {
			fr_id = BufFrmDescriptor.getFrameIDForPageID(globalPageId.pid);
			// System.out.println("The frame' pid is " +
			// bufDescr[fr_id].page_number.pid + " pin count is "
			// + bufDescr[fr_id].pin_count + " frame id is " + fr_id);
			//System.out.println("Cool we are done NOW 3 \n " + bufDescr[1]);
			if (bufDescr[fr_id].getPin_count() == 1) {
				unpinPage(globalPageId, false);
			} else if (bufDescr[fr_id].getPin_count() > 1) {
				throw new PagePinnedException(new Exception(), "ERROR MSG: pin count is not 0 but free page called. ");
			} else {

			}
		} catch (PagePinnedException e) {
			throw e;
		} catch (HashEntryNotFoundException e) {

		} 
		try {
			Minibase.DiskManager.deallocate_page(globalPageId);
		} catch (Exception e) {
			throw new ChainException(e, "ERROR MSG: deallocate page request to disk failed.");
		}
		// If successful then remove from the map
		if(fr_id != null)
		{
			BufFrmDescriptor bg = bufDescr[fr_id];
			assert bg.getPage_number().pid == globalPageId.pid;
			BufFrmDescriptor.removeFrameIDForPageID(bufDescr[fr_id].getPage_number().pid);
			bufDescr[fr_id].resetFrame();
		}
	};

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 *
	 * @param pageid
	 *            the page number in the database.
	 */
	public void flushPage(PageId pageid) throws ChainException {
		
		//System.out.println("CALLLEEED FLUSH PAGE " );
		int fr_id = BufFrmDescriptor.getFrameIDForPageID(pageid.pid);
		BufFrmDescriptor bg = bufDescr[fr_id];
		assert bufDescr[fr_id].getPage_number().pid == pageid.pid;
		if (bufDescr[fr_id].getPage_number().pid == pageid.pid) {
			//System.out.println("Cool we are done NOW 4 \n " + bufDescr[1]);
			if (bufDescr[fr_id].isDirty()) {
				Page newpage = new Page(bufDescr[fr_id].getFrame_data());
				try {
					Minibase.DiskManager.write_page(bufDescr[fr_id].getPage_number(), newpage);
				} catch (Exception e) {
					throw new ChainException(e, "ERROR MSG: write to disk failed.");
				}
				bufDescr[fr_id].setDirty(false);
			}
		}
	};

	/**
	 * Used to flush all dirty pages in the buffer pool to disk
	 *
	 */
	public void flushAllPages() throws ChainException {
		
		//System.out.println("CALLLEEED FLUSH ALL PAGES " );
		for (int a = 0; a < bufDescr.length; a++) {
			if (bufDescr[a].getPage_number() != null) {
				flushPage(bufDescr[a].getPage_number());
			}
		}
	};

	/**
	 * Returns the total number of buffer frames.
	 */
	public int getNumBuffers() {
		return bufDescr.length;
	}

	/**
	 * Returns the total number of unpinned buffer frames.
	 */
	public int getNumUnpinned() {
		int numUnpinned = 0;
		for (int a = 0; a < bufDescr.length; a++) {
			if (bufDescr[a].getPin_count() == 0) {
				numUnpinned++;
			}
		}
		return numUnpinned;
	}
};