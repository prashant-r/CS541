package query;

import parser.AST_DropIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

  public String idxname;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {

    idxname = tree.getFileName();
    QueryCheck.indexExists(idxname);
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HashIndex hashInd = new HashIndex(idxname);
    hashInd.deleteFile();
    Minibase.SystemCatalog.dropIndex(idxname);
    System.out.println("\n Index Dropped. "); 
  }
}
