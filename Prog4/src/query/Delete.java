package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import parser.AST_Delete;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;
import query.TupleOptions;
/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {
  private String tblName;
  private Predicate[][] predicates;
  private Schema schema;
  private HeapFile heapFile; 
  private HeapScan heapScan;


  public Delete(AST_Delete tree) throws QueryException {
    tblName = tree.getFileName();
    QueryCheck.tableExists(tblName);
    schema = Minibase.SystemCatalog.getSchema(tblName);
    predicates = tree.getPredicates();
    QueryCheck.predicates(schema, predicates);
  }
  
  public void execute() {
    int rowCount = 0;
    heapFile = new HeapFile(tblName);
    heapScan = heapFile.openScan();
    
    IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(tblName);
    while(heapScan.hasNext()) {
      RID rid = new RID();
      Tuple tuple = new Tuple(schema, heapScan.getNext(rid));
      rowCount = deletedTupleIfMatchesPredicates(indexes, predicates, tuple, rid) ? rowCount+1 : rowCount;
    }
    Minibase.SystemCatalog.delete(tblName, rowCount);
    System.out.println(rowCount + " rows deleted.");
    heapScan.close();
  }

  boolean deletedTupleIfMatchesPredicates(IndexDesc[] indexes, Predicate[][] predicates, Tuple tuple, RID rid)
  {
     if(TupleOptions.predicatesAreValidForOperation(predicates, tuple)) {
        TupleOptions.TupleDeleteByRID(indexes, tuple, rid);
        heapFile.deleteRecord(rid);
        return true;
      }
     return false;
  }
}
