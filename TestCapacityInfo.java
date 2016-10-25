package heap;

import global.PageId;

public class TestCapacityInfo {

    CapacityInfo capacityInfo= new CapacityInfo();

    public void SetUp()
    {
        capacityInfo.insert(Short.MIN_VALUE, new PageId(Integer.MIN_VALUE));
        capacityInfo.insert(Short.MAX_VALUE,new PageId(Integer.MAX_VALUE));
    }


    public boolean TestContainsKey()
    {
        return  capacityInfo.containsKey(Short.MAX_VALUE) && capacityInfo.containsKey(Short.MIN_VALUE);
    }

    public boolean TestContainsPageId()
    {
        return  capacityInfo.containsPageId(new PageId(Integer.MAX_VALUE)) && capacityInfo.containsPageId(new PageId(Integer.MIN_VALUE));
    }

    public  boolean TestContainsKeyAndPageId()
    {
        return capacityInfo.containsKeyAndPageId(Short.MIN_VALUE, new PageId(Integer.MIN_VALUE))
        && capacityInfo.containsKeyAndPageId(Short.MAX_VALUE , new PageId(Integer.MAX_VALUE));
    }

    public boolean TestIsNonEmpty()
    {
        return !capacityInfo.isEmpty();
    }

    public boolean TestRemoveKey()
    {
        capacityInfo.removeKey(Short.MAX_VALUE);

        return !capacityInfo.containsKey(Short.MAX_VALUE);
    }

    public boolean TestRemovePageId()
    {
        capacityInfo.removePageId(Short.MIN_VALUE, new PageId(Integer.MIN_VALUE));

        return !capacityInfo.containsKeyAndPageId(Short.MAX_VALUE , new PageId(Integer.MAX_VALUE));
    }


    public boolean TestGetPageWithAvailCapacity()
    {
        PageId pageId= null;
        PageId pageId1 =null;
        try {
            pageId= capacityInfo.getPageWithAvailCapacity(Short.MIN_VALUE);
            pageId1= capacityInfo.getPageWithAvailCapacity(Short.MAX_VALUE);
        }
        catch (Exception e)
        {
            return  false;
        }

        return pageId!=null && pageId1!=null;
    }

    public boolean TestMembership()
    {
        capacityInfo.insert(Short.MAX_VALUE,new PageId(Integer.MIN_VALUE));
        capacityInfo.insert(Short.MIN_VALUE,new PageId(Integer.MAX_VALUE));

        return (capacityInfo.membership().size()!=0);
    }

    public static void main(String args[])
    {
        TestCapacityInfo testCapacityInfo = new TestCapacityInfo();

        testCapacityInfo.SetUp();

        boolean result = testCapacityInfo.TestContainsKey() &&
                         testCapacityInfo.TestContainsKeyAndPageId() &&
                         testCapacityInfo.TestContainsPageId() &&
                         testCapacityInfo.TestIsNonEmpty() &&
                         testCapacityInfo.TestRemoveKey() &&
                         testCapacityInfo.TestRemovePageId() &&
                         testCapacityInfo.TestMembership() &&
                         testCapacityInfo.TestGetPageWithAvailCapacity() &&
                         !testCapacityInfo.TestIsNonEmpty();

        if(result)
            System.out.println("Capacity Info test cases passed");
        else
            System.out.println("Capacity Info test cases failed");
    }
}
