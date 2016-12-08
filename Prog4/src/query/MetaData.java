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

 public class MetaData{
    public boolean[] flags;
    public Map<String, Set<Integer>> map; 
    public MetaData(Predicate[][] predicates, Map<String, String> fieldToTableMap){
    this.flags = new boolean[predicates.length];
    this.map = new HashMap<String, Set<Integer>>();
    List<Integer> types = Arrays.asList(AttrType.INTEGER,AttrType.FLOAT,AttrType.STRING);
    for(int i=0;i<predicates.length;i++){
      String table = null;
      int j=0;
      for(j=0;j<predicates[i].length;j++){
        Integer rtype = predicates[i][j].getRtype();
        String field = predicates[i][j].getLeft().toString();
        table = table==null ? fieldToTableMap.get(field).toString() : table;
        if(!types.contains(rtype) || !table.equals(fieldToTableMap.get(field).toString())){
          break;
        }
      }
      if(j==predicates[i].length){
        if(this.map.get(table)==null)
          this.map.put(table, new HashSet<Integer>());
        this.map.get(table).add(i);
        this.flags[i]=true;
      }
    }
  }
 }