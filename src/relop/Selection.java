package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
	
	private Iterator scan;
	private Predicate[] predicates;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    scan = iter;
    schema = scan.schema;
    predicates = preds;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
// TODO: What to do??
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	 scan.restart();    
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //TODO: Dont kno if i return false,does that mean that there wud be no more tuples
	  // that satisfy the predicate, or there are no more tuples at all
    return scan.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
   scan.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	//TODO: Dont kno if i return false,does that mean that there wud be no more tuples
	  // that satisfy the predicate, or there are no more tuples at all
    return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
	public Tuple getNext() {
		Tuple t;
		//boolean flag = true;
		if (scan != null) {			
			while (scan.hasNext()) {
				//flag = true;
				t = scan.getNext();
				boolean eval = false;
				for (Predicate p : predicates) {
					eval = eval | p.evaluate(t);
					if (eval == true)
						break;
				}
				if (eval) {
					return t;
				}
			}
		}
		return null;
	}

} // public class Selection extends Iterator
