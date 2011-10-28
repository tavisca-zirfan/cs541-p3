package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {
	
	private Iterator liter;
	private Iterator riter;
	private Predicate[] predicates;	
	private Tuple ltuple;

  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
	  liter = left;
	  riter = right;
	  predicates = preds;
	  this.schema = Schema.join(left.schema, right.schema);
	  ltuple = liter.getNext();
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
    liter.restart();
    riter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return liter.isOpen() & riter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    liter.close();
    riter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    // Assuming the liter is the outer relation.
	// So, the join will have no more tuples when the outer relation ends.
	  return liter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
	public Tuple getNext() {
		// Tuple ltuple = new Tuple(lschema);
		Tuple rtuple;
		Tuple newtuple = new Tuple(schema);
		boolean eval = true;

		while (liter.hasNext()) {
			if (!riter.hasNext()) {
				ltuple = liter.getNext();
				riter.restart();
			}
			do {
				eval = true;
				rtuple = riter.getNext();
				newtuple = Tuple.join(ltuple, rtuple, schema);
				for (Predicate p : predicates) {
					if (!p.evaluate(newtuple)) {
						eval = false;
						break;
					}
				}
				if (eval) {
					break;
				}
			} while (riter.hasNext());
			if (eval) {
				return newtuple;
			}
		}
		return null;
	}

} // public class SimpleJoin extends Iterator
