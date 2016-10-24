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
		//System.out.println("Insert Request is " + x + " " + y.pid);
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
		
		//System.out.println("INFO is : " + info);
		membershipInfo.add(y);
		//System.out.println("MEMBERSHIP INFO is :" + membershipInfo );
	}
	
	public boolean containsKey(Short x)
	{
	//	System.out.println("containsKey Request is " + x );
		return capacityInfo.containsKey(x);
	}
	
	public boolean containsPageId(PageId pageid)
	{
		//System.out.println("containsPageId Request is " + pageid);
		return membershipInfo.contains(pageid);
	}
	
	public boolean containsKeyAndPageId(Short x, PageId pageId)
	{
	//	System.out.println("ContainsKeyAndPageId Request is " + x + " " + pageId);
		if(containsKey(x))
		{
			if(capacityInfo.get(x) == null || capacityInfo.get(x).isEmpty()){ System.out.println("CapacityInfo.java:63 - -Warning: non existent value for key");return false;}
			if(capacityInfo.get(x).contains(pageId))
			{
				return true;
			}
		}
		return false;
	}
	
	public void removeKey(Short x)
	{
		//System.out.println("Remove key Request is " + x);
		if(!containsKey(x)) System.out.println("CapacityInfo.java:75 - -Warning: trying to remove non existant key");
		if(capacityInfo.get(x)!= null && capacityInfo.get(x).size() > 0) System.out.println("CapacityInfo.java:64 - - Trying to delete non empty key");
		capacityInfo.remove(x);
		
	}
	
	public void removePageId(Short x, PageId pageid)
	{
		//System.out.println("RemovePageId Request is size " + x + " page id " +pageid );
		if(!containsKey(x)){
			System.out.println("CapacityInfo.java:84 - -ERROR: trying to remove non existant key");
			return;
		}
		if(capacityInfo.get(x) == null || capacityInfo.get(x).isEmpty()){ 
			System.out.println("CapacityInfo.java:85 - -ERROR: non existent value for key") ; 
			return;
		}
//		
//		System.out.println("State just before remove called .. .");
//		System.out.println(capacityInfo);
//		System.out.println(membershipInfo);
		
		capacityInfo.get(x).remove(pageid);
		if(capacityInfo.get(x) == null || capacityInfo.get(x).isEmpty()){ 
			removeKey(x);
		}
		
		membershipInfo.remove(pageid);
//		
//		System.out.println("State right after remove called .. .");
//		System.out.println(capacityInfo);
//		System.out.println(membershipInfo);
	}
	
	public PageId getPageWithAvailCapacity(Short cap) throws ChainException
	{
		//System.out.println("getPageWithAvail  Request is " + cap);
		// Note : can also use tree map function - ceilingEntry for same task.
		Entry<Short, TreeSet<PageId>> entry = capacityInfo.ceilingEntry(cap);
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
	
	public LinkedHashSet<PageId> membership()
	{
//		System.out.println("Iterator Request is " + membershipInfo);
//		System.out.println("The capacity info is " + capacityInfo);
		return membershipInfo;
	}
	
	public boolean isEmpty()
	{
	//	System.out.println("isEmpty Request is ");
		return membershipInfo.isEmpty();
	}
	
	
	public void reconstructMap(HFPage hfpage)
	{
	//	System.out.println("Reconstruct Map request with HFPage " + hfpage.getFreeSpace() + " " + hfpage.getCurPage().pid);
		insert(hfpage.getFreeSpace(),hfpage.getCurPage() );
	}

	@Override
	public String toString() {
		return "CapacityInfo [capacityInfo=" + capacityInfo + ", membershipInfo=" + membershipInfo + "]";
	}
	
	
}
