package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.BucketScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  private HashIndex hashIndex;
  private HeapFile heapFile;
  private RID lastRid;
  private BucketScan bucketScan;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
  
    this.hashIndex = index;
    this.heapFile = file;
    this.bucketScan = (hashIndex != null) ? hashIndex.openScan() : null;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
     System.out.println("Index Scan Iterator: ");
     indent(depth);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
   this.bucketScan = hashIndex != null ? hashIndex.openScan() : null;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return bucketScan != null;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
      if (bucketScan!= null) bucketScan.close();  
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return (bucketScan != null )? bucketScan.hasNext() : false; 
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    return (bucketScan.hasNext()) ? (new Tuple( schema, heapFile.selectRecord(bucketScan.getNext())) ): null;
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    return bucketScan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    return bucketScan.getNextHash();
  }

} // public class IndexScan extends Iterator
