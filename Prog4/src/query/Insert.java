package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;
/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  String tblName;
  Tuple tuple;
  Schema schm;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
    tblName = tree.getFileName();
    QueryCheck.tableExists(tblName);
    schm = Minibase.SystemCatalog.getSchema(tblName);
    QueryCheck.insertValues(schm, tree.getValues());
    tuple = new Tuple(schm, tree.getValues());

  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HeapFile heapFile = new HeapFile(tblName);
    Minibase.SystemCatalog.insert(tblName);
    RID rid  = heapFile.insertRecord(tuple.getData());
    for(IndexDesc ind : Minibase.SystemCatalog.getIndexes(tblName))
      new HashIndex(ind.indexName).insertEntry(new SearchKey(tuple.getField(ind.columnName)), rid);
    System.out.println("1 row inserted in " + tblName);

  } // public void execute()
} // class Insert implements Plan
