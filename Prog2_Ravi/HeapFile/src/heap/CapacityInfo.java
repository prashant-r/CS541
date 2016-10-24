package heap;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
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
	
	
	private TreeMap<Short, TreeSet<PageId>> info;
	private HashSet<PageId> membershipInfo;
	
	public CapacityInfo()
	{
		info =  new TreeMap<Short, TreeSet<PageId>>();
		membershipInfo = new HashSet<PageId>();
	}
	
	public void insert(Short x, PageId y)
	{
		if(info.containsKey(x))
		{
			info.get(x).add(y);
		}
		else
		{
			TreeSet<PageId> pageIds = new TreeSet<PageId>(new PageIdComparator());
			pageIds.add(y);
			info.put(x, pageIds);			
		}
		System.out.println(info);
		membershipInfo.add(y);
	}
	
	public boolean containsKey(Short x)
	{
		return info.containsKey(x);
	}
	
	public boolean containsPageId(PageId pageid)
	{
		return membershipInfo.contains(pageid);
	}
	
	public boolean containsKeyAndPageId(Short x, PageId pageId)
	{
		if(containsKey(x))
		{
			if(info.get(x) == null || info.get(x).isEmpty()){ System.out.println("CapacityInfo.java:63 - -Warning: non existent value for key");return false;}
			if(info.get(x).contains(pageId))
			{
				return true;
			}
		}
		return false;
	}
	
	public void removeKey(Short x)
	{
		System.out.println("Remove key " + x);
		if(!containsKey(x)) System.out.println("CapacityInfo.java:75 - -Warning: trying to remove non existant key");
		if(info.get(x)!= null && info.get(x).size() > 0) System.out.println("CapacityInfo.java:64 - - Trying to delete non empty key");
		info.remove(x);
		
	}
	
	public void removePageId(Short x, PageId pageid)
	{
		System.out.println("Remove size " + x + " page id " +pageid );
		if(!containsKey(x)) System.out.println("CapacityInfo.java:84 - -Warning: trying to remove non existant key");
		if(info.get(x) == null || info.get(x).isEmpty()){ System.out.println("CapacityInfo.java:85 - -Warning: non existent value for key") ; removeKey(x); return;}
		info.get(x).remove(pageid);
		membershipInfo.remove(pageid);
	}
	
	public PageId getPageWithAvailCapacity(Short cap) throws ChainException
	{
		// Note : can also use tree map function - ceilingEntry for same task.
		Entry<Short, TreeSet<PageId>> entry = info.ceilingEntry(cap);
		if(entry== null) return null;
		if(entry.getValue() == null) {
			System.out.println("CapacityInfo.java:96 - -Warning: trying to get null vaulue for existent key -  - Hint: Should remove key instead.");
			return null;
		}
		if(entry.getValue().isEmpty()) throw new ChainException(null ,"CapacityInfo.java:99 - -Warning:Poll attempted on empty LinkedList. "); 
		
		PageId toRemove = entry.getValue().pollFirst();
		if(entry.getValue().isEmpty()) removeKey(entry.getKey());
		membershipInfo.remove(toRemove);
		return toRemove;
	}
	
	public Iterator<PageId> iterator()
	{
		return membershipInfo.iterator();
	}
	
	public boolean isEmpty()
	{
		return membershipInfo.isEmpty();
	}
	
}
