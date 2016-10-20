package bufmgr;

import global.PageId;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * Created by henryjones on 18/10/16.
 */
public class TimeReferences
{
    public int pageId;
    public float crf=0;             //for starting case
    public boolean flag=false;

    ArrayList<Integer>time_value_list;

    public TimeReferences()
    {
        time_value_list= new ArrayList<Integer>();
    }

}
