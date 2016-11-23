package query;

import parser.AST_CreateIndex;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  public String idxname;
  public String tblname;
  public String colname;
  public Schema schema;
  public HashIndex newHashIndex;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {

    idxname = tree.getFileName();
    tblname = tree.getIxTable();
    schema = Minibase.SystemCatalog.getSchema(tblname);
    colname = tree.getIxColumn();

    // You must validate all query inputs
    QueryCheck.fileNotExists(idxname);
    QueryCheck.tableExists(tblname);
    QueryCheck.columnExists(schema, colname);

    newHashIndex = new HashIndex(idxname);

  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    //System.out.println("(Not implemented)");
    Minibase.SystemCatalog.createIndex(idxname, tblname, colname);
    HeapScan heapScan = new HeapFile(tablename).openScan();
    RID rid = new RID();
    while(heapScan.hasNext())
      hashind.insertEntry(new SearchKey(new Tuple(schema,heapScan.getNext(rid)).getField(colname)), rid);
    heapScan.close();
  } // public void execute()

} // class CreateIndex implements Plan
