package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  private Iterator scanIterator;
  private Tuple currentTuple;
  private Predicate[] predicates;
  boolean done;

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    this.scanIterator = iter;
    this.schema = iter.schema;
    this.predicates = preds;
    this.currentTuple = null;
    this.done = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {   
    indent(depth);
    System.out.println("Selection Iterator : ");
    scanIterator.explain(depth + 1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    scanIterator.restart();
    reset();
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

  private void reset()
  {
    predicates = null;
    currentTuple = null;
    done = true;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    boolean found = false;
    while(scanIterator.hasNext())
    {   
        found = true;
        Tuple nextTuple = scanIterator.getNext();
        for(Predicate pred : predicates)
          if(!pred.evaluate(nextTuple)) {
            found = false;
          }
          else
          {
            found = true; break;
          }
        if(found) {
          currentTuple = nextTuple;
          break; 
        } 
    }
    done = !found;
    if(done) currentTuple = null;
    return found;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(done) throw new IllegalStateException("Selection getNext() failed.");
    return currentTuple;
  }

} // public class Selection extends Iterator
