package bufmgr;

import global.Page;
import global.PageId;

import java.io.IOException;

import chainexception.ChainException;
import global.Minibase;

public class BufMgr {

	public EvictionPolicy policy;
	public BufFrmDescriptor[] bufDescr;
	int ctime;

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

		// Set the replacement policy
		switch (replacementPolicy) {
		case "LRFU":
			this.policy = new LRFUPolicy();
		default:
			this.policy = new LRFUPolicy();
		}

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

		// Check if this page is already in the buffer pool.
		Integer fr_id = BufFrmDescriptor.getFrameIDForPageId(pageno.pid);
		ctime++;

		// If it is, increment the pin_count
		if (fr_id != null) {
			BufFrmDescriptor bfd = bufDescr[fr_id];
			// if the pin count was 0 before the call
			if (bfd.pin_count == 0) {
				bfd.referenceTimes.clear();
			}
			bfd.pin_count = bfd.pin_count + 1;
			bfd.referenceTimes.add(ctime);
			page.setpage(bfd.frame_data);
		} else {
			// It is not in the pool, choose a page to replace.

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

		PageId pgId = new PageId();
		try {
			Minibase.DiskManager.allocate_page(pgId, howmany);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new ChainException(e, "ERROR MSG : allocate page request to disk failed.");
		}
		try {
			pinPage(pgId, firstpage, global.GlobalConst.PIN_MEMCPY);
		} catch (Exception e) {
			try {
				Minibase.DiskManager.deallocate_page(pgId, howmany);
			} catch (Exception eprime) {
				throw new ChainException(eprime, "ERROR MSG: deallocate several pages request in disk failed.");
			}
			return null;
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

		try {
			Minibase.DiskManager.deallocate_page(globalPageId);
		} catch (Exception e) {
			throw new ChainException(e, "ERROR MSG: deallocate page request to disk failed.");
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

		int fr_id = BufFrmDescriptor.getFrameIDForPageId(pageid.pid);
		if (bufDescr[fr_id].page_number.pid == pageid.pid) {
			if (bufDescr[fr_id].dirty) {
				Page newpage = new Page(bufDescr[fr_id].frame_data);
				try {
					Minibase.DiskManager.write_page(bufDescr[fr_id].page_number, newpage);
				} catch (Exception e) {
					throw new ChainException(e, "ERROR MSG: write to disk failed.");
				}
			}
		}
	};

	/**
	 * Used to flush all dirty pages in the buffer pool to disk
	 *
	 */
	public void flushAllPages() throws ChainException {
		for (int a = 0; a < bufDescr.length; a++) {
			if (bufDescr[a].page_number != null) {
				flushPage(bufDescr[a].page_number);
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
			if (bufDescr[a].pin_count == 0) {
				numUnpinned = 0;
			}
		}
		return numUnpinned;
	}
};