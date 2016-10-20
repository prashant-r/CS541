package bufmgr;

public interface EvictionPolicy {

		public void updateFrame(BufFrmDescriptor buf);
		public void evict();
}
