package query;

import global.AttrType;
import global.Minibase;
import parser.AST_Describe;
import relop.Schema;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

  private Schema schm;
  public Describe(AST_Describe tree) throws QueryException {
    String tblName = tree.getFileName();
    QueryCheck.tableExists(tblName);
    schm = Minibase.SystemCatalog.getSchema(tblName);
  }
  public void execute() {
    for(int i=0;i<schm.getCount();i++){

      System.out.println(String.format("%-5s  %-8s", schm.fieldName(i), AttrType.toString(schm.fieldType(i))));
    }
    System.out.println("0 rows affected");
  }
}