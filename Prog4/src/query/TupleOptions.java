package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import parser.AST_Update;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

class TupleOptions
{
	public static void TupleDeleteByRID(IndexDesc[] indexes, Tuple tuple, RID rid)
	{
	 for(IndexDesc ind : indexes) {
          HashIndex hIndex = new HashIndex(ind.indexName);
          SearchKey key = new SearchKey(tuple.getField(ind.columnName));
          hIndex.deleteEntry(key, rid);
        }
    }
    public static void TupleInsertByRID(IndexDesc[] indexes, Tuple tuple, RID rid)
    {
     for(IndexDesc ind : indexes) {
          HashIndex hIndex = new HashIndex(ind.indexName);
          SearchKey key = new SearchKey(tuple.getField(ind.columnName));
          hIndex.insertEntry(key, rid);
        }
    }
  	public static boolean predicatesAreValidForOperation(Predicate[][] predicates, Tuple tuple)
  	{
    	for(Predicate[] currPreds : predicates)
      	for(Predicate currPred : currPreds)
        	if(!currPred.evaluate(tuple)) return false;
      return true;
  	}

}