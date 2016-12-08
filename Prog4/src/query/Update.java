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
import query.TupleOptions;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

  private String tblName;
  private int[] fieldNumbersToUpdate;
  private Schema schema;
  private Object[] values;
  private Predicate[][] predicates;
  private HeapFile heapFile;
  private HeapScan heapScan;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException
   *             if invalid column names, values, or predicates
   */
  public Update(AST_Update tree) throws QueryException {
    tblName = tree.getFileName();
    QueryCheck.tableExists(tblName);
    schema = Minibase.SystemCatalog.getSchema(tblName);
    String[] columns = tree.getColumns();
    QueryCheck.updateFields(schema, columns);
    fieldNumbersToUpdate = new int[columns.length];
    for (int i = 0; i < columns.length; i++) {
      String column = columns[i];
      fieldNumbersToUpdate[i] = schema.fieldNumber(column);
    }
    values = tree.getValues();
    QueryCheck.updateValues(schema, fieldNumbersToUpdate, values);
    predicates = tree.getPredicates();
    QueryCheck.predicates(schema, predicates);
  }

  public void execute() {
    int rowCount = 0;
    heapFile = new HeapFile(tblName);
    heapScan = heapFile.openScan();
    IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(tblName, schema,fieldNumbersToUpdate);
    while(heapScan.hasNext()) {
      RID rid = new RID();
      Tuple tuple = new Tuple(schema, heapScan.getNext(rid));
      rowCount = updatedTupleIfMatches(indexes, predicates, tuple, rid) ? rowCount + 1 : rowCount;
    }
    heapScan.close();
    System.out.println(rowCount + " rows updated.");

  } 

  boolean updatedTupleIfMatches(IndexDesc[] indexes, Predicate[][] predicates, Tuple tuple, RID rid)
  {
    if(TupleOptions.predicatesAreValidForOperation(predicates, tuple)) {
        TupleOptions.TupleDeleteByRID(indexes, tuple, rid);
        for (int i = 0; i < fieldNumbersToUpdate.length; i++) {
          tuple.setField(fieldNumbersToUpdate[i], values[i]);
        }
        heapFile.updateRecord(rid, tuple.getData());
        TupleOptions.TupleInsertByRID(indexes, tuple, rid);
        return true;
      }
    return false;
  }
}