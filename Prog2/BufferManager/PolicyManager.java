package bufmgr;

import global.PageId;

import javax.swing.text.html.HTMLDocument;
import java.sql.Array;
import java.sql.Time;
import java.util.*;

public class PolicyManager
{
    private ArrayList<Entry> entries;
    private Queue<Integer> queue;
    private int numBufs;
    private String replaceArg;
    private int reqIndex;
	private static float time_value;
	private List<TimeReferences> listOfLists;

	public PolicyManager(String replaceArg,int numBufs)
	{
	    this.numBufs = numBufs;
	    this.replaceArg = replaceArg;
	    queue = new LinkedList<Integer>();
	    entries = new ArrayList<Entry>();
		listOfLists = new ArrayList<TimeReferences>();
	}

	public void addReplacementCandidate(int frameNumber)
	{
	    entries.add((new Entry(frameNumber)));
	}

	public int getReplacement(Descriptor bufDescr[],HashMap<Integer,Integer> hashTable, PageId pgid)
	{

        switch(replaceArg)
        {
            case "Clock":       return FIFO_Replacement();
            case "LRU":         return LRU_Replacement(bufDescr,false);
            case "MRU":         return MRU_Replacement(bufDescr,false);
			case "LRFU":		return  myReplacement(pgid.pid,bufDescr);
        }
		return -1 ;
	}

	public int FIFO_Replacement()
	{
		if(queue.size() != 0) // get the 1st element in the queue
		{
			int frameNumber = queue.poll();
	    	return frameNumber;
		}
	    return -1; // queue is empty < fayaset >
	}

//	public int LRFU_Replacement(HashMap<Integer,Integer> hashTable,PageId pgid) {
//
//		time_value++;
//		int pageid_to_return=0;
//		int time_list_index=0;
//		int i;
//		boolean found = false;
//
//		TimeReferences time = null;
//
//		System.out.println("The page to be inserted is "+pgid);
//
//		if (listOfLists.isEmpty()) {
//			for (i = 0; i < entries.size(); i++) {
//				int id = entries.get(i).getFrameNumber();
//
//				TimeReferences temp = new TimeReferences();
//				temp.crf = i + 1;
//				temp.flag=true;
//				temp.time_value_list.add(i + 1);
//				temp.pageId = id;
//
//				listOfLists.add(temp);
//
//
//			}
//			time_value = i;
//		}
//
//		if (!listOfLists.isEmpty()) {
//
//			for (int k = 0; k < listOfLists.size(); k++) {
//
//				time = (TimeReferences) listOfLists.get(k);
//
//				if (time.pageId == pgid.pid) {
//					found = true;
//					time_list_index = (int) k;
//					break;
//				}
//			}
//
//
//			System.out.println("This list contains..");
//
//
//
//
//			if (found) {
//
//				time.time_value_list.add(time_value);    // add time to the list of times for that page in a temp oage
//				time.crf = calculate_CRF(time, time_value);// calculates and stores CRF
//				pageid_to_return = time.pageId;
//
//				System.out.println("The page returned was " + pageid_to_return);
//
//
//				//listOfLists.remove(time_list_index);		//remove the old page
//				//listOfLists.add(time_list_index,time);		//add the page with the new updated time list
//
//				if(hashTable.containsKey((pageid_to_return)))
//					return hashTable.get(pageid_to_return);
//
//
////			time_value++;
////
////			List<Float> crflist = new ArrayList<Float>();
////
////			for (Iterator k = listOfLists.iterator(); k.hasNext(); ) {
////				time = (TimeReferences) k.next();
////				//if (time.pageId == pgid.pid) {
////					crflist.add(time.crf);
////				//}
//			}
//
//			if (!found) {
//
//				time = new TimeReferences();
//				time.crf = time_value;
//				time.pageId = pgid.pid;
//
//				time.time_value_list.add(time_value);
//				//time_value++;
//				listOfLists.add(time);
////
//
//
//			}
//
//
//
//		}
//
//		TimeReferences time_temp2=null;
//
//		float min_crf = Float.POSITIVE_INFINITY;
//
//
//
//		for(Iterator k = listOfLists.iterator(); k.hasNext();)
//		{
//
//			time_temp2 = (TimeReferences) k.next();
//			//pageid_to_return= time_temp2.pageId;
//
//			if (time_temp2.crf < min_crf && time_temp2.flag==true)
//			{
//				pageid_to_return = time_temp2.pageId;
//				min_crf = time_temp2.crf;
//
//			}
//		}
//
//		time_temp2.flag=false;
//		System.out.println("Minimum crf is" + min_crf);
//		System.out.println("XXX Page Id" + (pageid_to_return));
//
////			Object[] a = crflist.toArray();
////
////			float lastElement = (float) a[crflist.size() - 1];
////			int p = 0;
////
////			for (p = 0; p < crflist.size() - 1; p++) {
////				a[p] = (float) 1 / ((lastElement - (float) a[p]) + 1);
////			}
////
////			p = 0;
////
////			for (Iterator k = listOfLists.iterator(); k.hasNext(); ) {
////				time = (TimeReferences) k.next();
////				if (time.pageId == pgid.pid) {
////					time.crf = (float) a[p];
////					p++;
////				}
////			}
////
////
////		} else {
////			time = new TimeReferences();
////			time.crf = time_value;
////			time.pageId = pgid.pid;
////
////			time.time_value_list.add(time_value);
////			time_value++;
////			listOfLists.add(time);
////
////		}
////
////		float[] array = new float[listOfLists.size()];
////		int j=0;
////
////		for(Iterator k= listOfLists.iterator(); k.hasNext();)
////		{
////			time = (TimeReferences) k.next();
////			array[j]= time.crf;
////			j++;
////		}
////
////		Arrays.sort(array);
////
////		for(Iterator k= listOfLists.iterator(); k.hasNext();)
////		{
////			time = (TimeReferences) k.next();
////
////			if (time.crf == array[0])
////				return time.pageId -1;
////
////			return -1;
////		}
//		for (Iterator k = listOfLists.iterator(); k.hasNext(); )
//		{
//			TimeReferences timeref= (TimeReferences)k.next();
//
//			System.out.print(timeref.pageId + " \t");
//			System.out.println(timeref.crf + " \t");
//		}
//
//		System.out.println("The page returned was "+ (pageid_to_return));
//
//		if(hashTable.containsKey((pageid_to_return)))
//			return hashTable.get(pageid_to_return);
//
//		return -1;
//
//	}





	public float calculate_CRF(ArrayList<Float> tref)
	{

		if(tref.size()==1)
			return tref.get(tref.size()-1);


		//float f= 0;
		float current_time=tref.get(tref.size()-1);

		float sum=0;


		for(int k=0;k<tref.size()-1;k++)				//temp.size()-1 because CRF does not take the last element
		{
			sum+=1/(current_time-tref.get(k)+1);
		}


		return sum;
	}









	public int LRU_Replacement(Descriptor bufDescr[] , boolean callMethod)
	{
	    long currentNumber;
	    long min = Entry.numberGenerator+1;
	    int frameNumber = -1;
	    int index = -1;
	    for (int i = 0 ; i < entries.size(); i++)
	    {
	       currentNumber = entries.get(i).getNumber();

	       if ( currentNumber < min)
	       {
	           min = currentNumber;
	           frameNumber = entries.get(i).getFrameNumber();
	           index = i;
	       }
	    }
	    reqIndex = index;
	    // 	if not called by love/hate then remove the element from the policy
	    if(frameNumber!=-1 && !callMethod) entries.remove(index);

	    return frameNumber;
	}
	public int MRU_Replacement(Descriptor bufDescr[], boolean callMethod)
	{
	    long currentNumber;
	    long max = -Entry.numberGenerator;
	    int frameNumber = -1 ;
	    int index = -1;
	    for (int i = 0 ; i < entries.size(); i++)
	    {
	       currentNumber = entries.get(i).getNumber();

	       if ( currentNumber > max)
	       {
	           max = currentNumber;
	           frameNumber = entries.get(i).getFrameNumber() ;
	           index = i;
	       }
	    }
	    reqIndex = index;
	    //if not called by love/hate then remove the element from the policy
	    if(frameNumber!=-1 && !callMethod) entries.remove(index);

	    return frameNumber;
	}


	public void removeAndShift(int frameNumber)
	{
		for( int i = 0 ; i < entries.size() ; i++ )
		{
			if(entries.get(i).getFrameNumber() == frameNumber)  entries.remove(i);
		}

	}



	public int myReplacement(int pgid,Descriptor bufDescr[])
	{

		HashMap<Integer,myPage>hashMapframeTable=new HashMap<Integer,myPage>();		//create replicated table
		HashMap<Integer,ArrayList<Float>> time_list= new HashMap<Integer,ArrayList<Float>>(); // HashMap to keep time record
		time_value++;



		if(hashMapframeTable.isEmpty())
		{
			for (int i = 0; i < entries.size(); i++) {
				int framenumber = entries.get(i).getFrameNumber();
				int pagenumber = bufDescr[framenumber].getPageNumber();

				//time_value++;                                        // for incrementing CRF

				myPage page = new myPage();                            //creat new page to add
				page.pageid = pagenumber;
				page.crf = time_value;

				hashMapframeTable.put(framenumber, page);                        //adding in replicated table

				ArrayList<Float> timeArrayList = new ArrayList<Float>();    //creating time list
				timeArrayList.add(page.crf);

				time_list.put(page.pageid, timeArrayList);                        //add the element
			}

			return entries.get(0).getFrameNumber();
		}

		else
		{
			if (time_list.containsKey(pgid)) {
				time_list.get(pgid).add(time_value);


				if(hashMapframeTable.containsKey(pgid))
				{
					float crf;
					ArrayList<Float>temp=time_list.get(pgid);
					crf=calculate_CRF(temp);

					myPage updater=new myPage();
					updater.pageid=pgid;
					updater.crf=crf;

					hashMapframeTable.replace(pgid,updater);

					myPage tempx=null;							//section to get frame number
					for(int x:hashMapframeTable.keySet())
					{
						tempx=hashMapframeTable.get(x);

						if (tempx.pageid==pgid)
						{
							return  x;

						}

					}


				}

				else
				{
					int mincrf_pageid;
					mincrf_pageid=calc_Min_CRF(hashMapframeTable);  //gives FRAME Number and not pag no.


					myPage mypage= new myPage();
					mypage.pageid= pgid;
					mypage.crf= time_value;


					hashMapframeTable.replace(mincrf_pageid, mypage);


					return mincrf_pageid;

				}

			}
			else {


				ArrayList<Float> timeArrayList = new ArrayList<Float>();    //creating time list

				timeArrayList.add(time_value);
				time_list.put(pgid, timeArrayList);                        //add the element

				int mincrf_pageid;
				mincrf_pageid=calc_Min_CRF(hashMapframeTable);  //gives FRAME Number and not pag no.


				myPage mypage= new myPage();
				mypage.pageid= pgid;
				mypage.crf= time_value;


				hashMapframeTable.replace(mincrf_pageid, mypage);


				return mincrf_pageid;



			}

		}

		return -1;
	}




		public int calc_Min_CRF(HashMap<Integer,myPage> tempHashMap)
		{
			float min=Float.POSITIVE_INFINITY;
			myPage temp=null;
			int mincrf_pagid=-1;
			for(int x:tempHashMap.keySet())
			{
				temp=tempHashMap.get(x);

				if (temp.crf<min)
				{
					min=temp.crf;
					mincrf_pagid=temp.pageid;
				}

			}

			for(int x:tempHashMap.keySet())
			{
				temp=tempHashMap.get(x);

				if (temp.pageid==mincrf_pagid)
				{

					return  x;

				}

			}

			return -1;

		}








		


}