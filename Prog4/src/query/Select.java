package query;

import global.AttrType;
import global.Minibase;
import heap.HeapFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import query.MetaData;
/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {
  private Iterator iter;
  private class TableMetaData{
    String name;
    Integer size;
    public TableMetaData(String name, Integer size) {
      this.name=name;
      this.size=size;
    }
  }
  private class TableMetaDataComparator implements Comparator<TableMetaData>
  {
    @Override
    public int compare(TableMetaData o1, TableMetaData o2) {
         return o1.size.compareTo(o2.size);
    }
  }
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
    String[] tables = tree.getTables();
    QueryCheck.tableExists(tables[0]);
    Schema predictedSchema = Minibase.SystemCatalog.getSchema(tables[0]);
    for(int i=1;i<tables.length;i++){
      QueryCheck.tableExists(tables[i]);
      Schema schema = Minibase.SystemCatalog.getSchema(tables[i]);
      predictedSchema = Schema.join(predictedSchema, schema);
    }

    Predicate[][] predicates = tree.getPredicates();
    for(int i=0;i<predicates.length;i++){
      QueryCheck.predicates(predictedSchema, predicates);
    }
    String[] columns = tree.getColumns();
    for(int i=0;i<columns.length;i++){
      QueryCheck.columnExists(predictedSchema, columns[i]);
    }
    List<TableMetaData> tableInfos = new ArrayList<TableMetaData>();
    for(int i=0;i<tables.length;i++){
      tableInfos.add(new TableMetaData(tables[i], Minibase.SystemCatalog.numRecords(tables[i], true)));
    }
    Collections.sort(tableInfos, new TableMetaDataComparator());
    Map<String, String> fieldToTableMap = parseSchemas(tables);
    MetaData metaData = new MetaData(predicates, fieldToTableMap);
    int counter =0 ;
    for(TableMetaData tableMeta : tableInfos){

      Schema schema = Minibase.SystemCatalog.getSchema(tableMeta.name);
      HeapFile file = new HeapFile(tableMeta.name);
      Iterator fileScan = new FileScan(schema, file);
      if(metaData.map.containsKey(tableMeta.name)){
        Set<Integer> indices = metaData.map.get(tableMeta.name);
        for(Integer index : indices){
          fileScan = new Selection(fileScan, predicates[index]);
        }
      }
      if(counter++==0){
        iter = fileScan;
      } else{
        Predicate[] preds = new Predicate[0];
        iter = new SimpleJoin(iter, fileScan, preds);
      }
    }
    for(int i=0;i<predicates.length;i++){
      if(!metaData.flags[i])
        iter = new Selection(iter, predicates[i]);
    }
    columns = tree.getColumns();
    if(columns.length!=0)
    {   
      Integer[] fields = new Integer[columns.length];
      for(int i=0;i<columns.length;i++){
        Schema schema = iter.getSchema();
        fields[i]=schema.fieldNumber(columns[i]);
      }
      iter = new Projection(iter, fields);
    }
  }

  private Map<String, String> parseSchemas(String[] tables){
    Map<String, String> map = new HashMap<String, String>();
    for(int i=0;i<tables.length;i++){
      Schema schema =  Minibase.SystemCatalog.getSchema(tables[i]);
      for(int j=0; j<schema.getCount(); j++){
        map.put(schema.fieldName(j), tables[i]);
      }
    }
    return map;
  }
  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    iter.explain(0);
    iter.execute();
    iter.close();
    System.out.println("0 rows affected.");

  } 
}