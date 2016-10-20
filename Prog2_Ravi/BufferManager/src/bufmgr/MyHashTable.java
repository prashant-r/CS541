package bufmgr;

import java.util.ArrayList;
import java.util.List;

public class MyHashTable {

	private final int a = 6;
	private final int b = 35;
	private final int htsize = 127;

	private class BucketElement {
		public Integer pageID;
		public Integer frameID;

		public BucketElement(Integer pageID, Integer frameID) {
			super();
			this.pageID = pageID;
			this.frameID = frameID;
		}

	}

	private class Bucket {
		private List<BucketElement> entries;

		public Bucket() {
			super();
			entries = new ArrayList<BucketElement>();
		}

		public void insert(Integer pageId, Integer frameId) {
			entries.add(new BucketElement(pageId, frameId));
		}
	}

	private class Directory {
		Bucket[] buckets;
	}

	private Directory directory;

	public MyHashTable() {
		directory = new Directory();
		directory.buckets = new Bucket[htsize];
		for (int a = 0; a < htsize; a++) {
			directory.buckets[a] = new Bucket();
		}
	}

	private int getBucketIndex(int value) {
		return (a * value + b) % htsize;
	}

	public void put(Integer pid, Integer fid) {
		int index = getBucketIndex(pid);
		directory.buckets[index].insert(pid, fid);
	}

	public Integer get(Integer pid) {
		int index = getBucketIndex(pid);
		for (BucketElement element : directory.buckets[index].entries) {
			if (element.pageID.equals(pid)) {
				return element.frameID;
			}
		}
		return null;
	}

	public void remove(Integer pid) {
		int index = getBucketIndex(pid);
		int elemIndex = 0;
		for (BucketElement element : directory.buckets[index].entries) {
			if (element.pageID.equals(pid)) {
				directory.buckets[index].entries.remove(elemIndex);
				break;
			}
			elemIndex++;
		}

	}

	public boolean containsKey(Integer pid) {

		int index = getBucketIndex(pid);
		for (BucketElement element : directory.buckets[index].entries) {
			if (element.pageID.equals(pid)) {
				return true;
			}
		}
		return false;
	}

	public void clear() {
		for (int a = 0; a < htsize; a++) {
			if(!directory.buckets[a].entries.isEmpty())
				directory.buckets[a].entries.clear();
		}
	}

}
