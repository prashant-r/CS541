package bufmgr;

import chainexception.ChainException;

public class PagePinnedException extends ChainException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public PagePinnedException(Exception e, String message) {
	    super(e, message);
	}

}
