package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
	
	private Iterator scan;
	private Predicate[] predicates;
	private Tuple _saved;
	private boolean _isClosed;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    scan = iter;
    schema = scan.schema;
    predicates = preds;
    _saved = null;
    _isClosed = false;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  // TODO: What to do??
      System.out.println("Selection");
      indent(depth);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	 scan.restart();    
	 _saved = null;
	 _isClosed = false;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //TODO: Dont kno if i return false,does that mean that there wud be no more tuples
	  // that satisfy the predicate, or there are no more tuples at all
    //return scan.isOpen();
	  return !_isClosed;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  scan.close();
	  _saved = null;
	  _isClosed = true;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	/*
	 * If you have a valid _saved, return that,
	 * else dig further with help of iterators and see if there is any existing tuple that satisfy the predicate which u can 
	 * deliver to caller and save that in _saved
	 */
	  
	  if (_saved != null)
		  return true;
	  else {
		  Tuple t;
			if (scan != null) {
				while (scan.hasNext()) {
					t = scan.getNext();
					boolean eval = false;			// done on based of OR operator, so its OK if any of predicate is true
					for (Predicate p : predicates) {
						eval = eval | p.evaluate(t);
						if (eval == true)
							break;
					}
					if (eval) {
						_saved = t;
						return true;
					}
				}
			}
		  return false;
	  }
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
	public Tuple getNext() {
		/*
		 * if you have a valid _saved, return that
		 * else, put the same logic in hasNext() here.
		 */
		if (_saved != null){
			Tuple ret = _saved;
			_saved = null;
			return ret;
		}
		
		Tuple t;
		if (scan != null) {
			while (scan.hasNext()) {
				t = scan.getNext();
				boolean eval = false;
				for (Predicate p : predicates) {
					eval = eval | p.evaluate(t);
					if (eval == true)
						break;
				}
				if (eval) {
					return t;
					// no need to put in _saved
				}
			}
		}
		throw new IllegalStateException("No more tuple left.");
	}

} // public class Selection extends Iterator
