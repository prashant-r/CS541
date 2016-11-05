package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  private Iterator scanIterator;
  private Tuple currentTuple;
  private Integer[] fields;
  boolean done;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
      this.schema = new Schema(fields.length);
      this.scanIterator = iter;
      this.fields = fields;
      this.done = true;
      this.currentTuple = null;
      // initialize the schema
      int fieldCounter = 0;
      for(Integer field : fields)
        schema.initField(fieldCounter++ , scanIterator.schema, field);
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
      indent(depth);
      System.out.println("Projection Iterator : ");
      scanIterator.explain(depth + 1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    scanIterator.restart();
    reset();
  }


  private void reset()
  {
    currentTuple = null;
    fields = null;
    done = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return scanIterator.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    scanIterator.close();
    reset();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    if(scanIterator.hasNext())
    {
      Tuple nextTuple = scanIterator.getNext();

      // Project only the fields we want from the scan Iterator we were given.

      currentTuple = new Tuple(schema);
      int counterTuple = 0;
      for(Integer field : fields)
      {
        currentTuple.setField(counterTuple++, nextTuple.getField(field));
      }
      done = false;
      return true;
    }
    done = true;
    currentTuple = null;
    return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(done) throw new IllegalStateException("Projection getNext() failed. ");
    return currentTuple;
  }

} // public class Projection extends Iterator
