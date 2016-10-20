package bufmgr;

import java.util.PriorityQueue;

public class LRFUPolicy implements EvictionPolicy {
	
	
	private PriorityQueue<BufFrmDescriptor> evictionQueue;
	
	@Override
	public void evict() {
		
	}
	
	@Override 
	public void updateFrame(BufFrmDescriptor buf)
	{
		
	}
	
	public LRFUPolicy()
	{
		evictionQueue = new PriorityQueue<BufFrmDescriptor>();
	}
}
