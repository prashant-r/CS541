package relop;

import global.SearchKey;
import index.HashScan;
import heap.HeapFile;
import index.HashIndex;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

  private HashIndex hashIndex;
  private HashScan hashScan;
  private SearchKey searchKey;
  private HeapFile heapFile;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
    this.schema = schema;
    this.hashIndex = index;
    this.searchKey = key;
    this.heapFile = file;
    if(hashIndex != null) hashScan = hashIndex.openScan(searchKey);
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {

    System.out.println("KeyScan iterator : ");
    indent(depth);

  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    if(hashIndex != null) hashScan = hashIndex.openScan(searchKey);
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return hashScan!= null;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    if(hashScan != null ) hashScan.close();
    hashScan = null;
    hashIndex = null;
    searchKey = null;
    heapFile = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return hashScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
     return new Tuple( schema, heapFile.selectRecord(hashScan.getNext())) ;
  }




} // public class KeyScan extends Iterator
