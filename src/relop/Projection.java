package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	private Iterator scan;
	private Integer[] fields;
	private Schema newSchema;
	
	
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    scan = iter;
    schema = iter.schema;
    this.fields = fields;
    
    newSchema = new Schema (fields.length);
    int newFldCnt = 0;
    for (Integer i : fields)
    	newSchema.initField(newFldCnt++, scan.schema, (i));
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    //TODO: What do to???
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
    return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    Tuple old_tuple;
    Tuple new_tuple = new Tuple(newSchema);
    newSchema.print();
    while(hasNext()){
    	old_tuple = scan.getNext();
    	int newFldCnt = 0;
    	for(Integer i : fields){
    		new_tuple.setField(newFldCnt++, old_tuple.getField(i));
    	}
    	return new_tuple;
    }
    return null;
  }

} // public class Projection extends Iterator

    
    
    
    
    