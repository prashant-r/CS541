package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  private HeapScan heapScan;
  private RID lastRid;
  private HeapFile heapFile;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
      this.heapFile = file;
      this.heapScan = file.openScan();
      this.schema = schema;
      this.lastRid = new RID();
  }

  public HeapFile getHeapFile()
  {
    return heapFile;
  }


  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
      System.out.println("File Scan Iterator: ");
      indent(depth);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
      this.heapScan = heapFile.openScan();
      this.lastRid = new RID();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return heapScan != null;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    if (heapScan != null ) heapScan.close();
    lastRid = null;
    heapFile = null;
    heapScan = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return heapScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    return new Tuple(schema,  heapScan.getNext(lastRid));
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return lastRid;
  }

} // public class FileScan extends Iterator
 