package heap;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import chainexception.ChainException;
import global.PageId;

public class CapacityInfo
{
	private class PageIdComparator implements Comparator<PageId> {
		@Override
		public int compare(PageId tv1, PageId tv2) {
			return (new Integer(tv1.pid).compareTo(tv2.pid));
		}
	}
	private TreeMap<Short, TreeSet<PageId>> capacityInfo;
	private LinkedHashSet<PageId> membershipInfo;
	
	public CapacityInfo()
	{
		capacityInfo =  new TreeMap<Short, TreeSet<PageId>>();
		membershipInfo = new LinkedHashSet<PageId>();
	}
	
	public void insert(Short x, PageId y)
	{
		if(capacityInfo.containsKey(x))
		{
			capacityInfo.get(x).add(y);
		}
		else
		{
			TreeSet<PageId> pageIds = new TreeSet<PageId>(new PageIdComparator());
			pageIds.add(y);
			capacityInfo.put(x, pageIds);			
		}
		membershipInfo.add(y);
	}
	
	public boolean containsKey(Short x)
	{
		return capacityInfo.containsKey(x);
	}
	
	public boolean containsPageId(PageId pageid)
	{
		return membershipInfo.contains(pageid);
	}
	
	public boolean containsKeyAndPageId(Short x, PageId pageId)
	{
		if(containsKey(x))
		{
			if(capacityInfo.get(x) == null || capacityInfo.get(x).isEmpty()){ return false;}
			if(capacityInfo.get(x).contains(pageId))
			{
				return true;
			}
		}
		return false;
	}
	
	public void removeKey(Short x)
	{
		assert(containsKey(x));
		assert((capacityInfo.get(x)!= null && capacityInfo.get(x).size() > 0) == false);
		capacityInfo.remove(x);
		
	}
	
	public void removePageId(Short x, PageId pageid)
	{
		if(!containsKey(x)){
			return;
		}
		if(capacityInfo.get(x) == null || capacityInfo.get(x).isEmpty()){ 
			return;
		}
		capacityInfo.get(x).remove(pageid);
		if(capacityInfo.get(x) == null || capacityInfo.get(x).isEmpty()){ 
			removeKey(x);
		}
		membershipInfo.remove(pageid);
	}
	
	public PageId getPageWithAvailCapacity(Short cap) throws ChainException
	{
		Entry<Short, TreeSet<PageId>> entry = capacityInfo.ceilingEntry(cap);
		if(entry== null) return null;
		if(entry.getValue() == null) {
			return null;
		}
		if(entry.getValue().isEmpty()) throw new ChainException(null ,"CapacityInfo.java:99 - -Warning:Poll attempted on empty LinkedList. "); 	
		PageId toRemove = entry.getValue().pollFirst();
		if(entry.getValue().isEmpty()) removeKey(entry.getKey());
		membershipInfo.remove(toRemove);
		return toRemove;
	}
	
	public LinkedHashSet<PageId> membership()
	{
		return membershipInfo;
	}
	
	public boolean isEmpty()
	{
		return membershipInfo.isEmpty();
	}
	
	
	public void reconstructMap(HFPage hfpage)
	{
		insert(hfpage.getFreeSpace(),hfpage.getCurPage() );
	}

	@Override
	public String toString() {
		return "CapacityInfo [capacityInfo=" + capacityInfo + ", membershipInfo=" + membershipInfo + "]";
	}
	
	
}
