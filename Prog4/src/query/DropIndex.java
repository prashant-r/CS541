package query;

import parser.AST_DropIndex;

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
  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    //System.out.println("(Not implemented)");
    HashIndex hashInd = new HashIndex(idxname);
    hashInd.deleteFile();
    Minibase.SystemCatalog.dropIndex(idxname);
    
  } // public void execute()

} // class DropIndex implements Plan
