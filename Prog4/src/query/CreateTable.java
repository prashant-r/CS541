package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_CreateTable;
import parser.ParseException;
import relop.Schema;

/**
 * Execution plan for creating tables.
 */
class CreateTable implements Plan {

  /** Name of the table to create. */
  protected String fileName;

  /** Schema of the table to create. */
  protected Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if file name already exists or schema is invalid
   */
  public CreateTable(AST_CreateTable tree) throws QueryException {
    fileName = tree.getFileName();
    QueryCheck.fileNotExists(fileName);
    try {
      schema = tree.getSchema();
    } catch (ParseException exc) {
      throw new QueryException(exc.getMessage());
    }

  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    new HeapFile(fileName);
    Minibase.SystemCatalog.createTable(fileName, schema);
    System.out.println("Table created.");
  }

}