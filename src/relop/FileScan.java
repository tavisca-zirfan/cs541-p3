package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
	
	private HeapScan scan;
	private HeapFile file;
	private RID lastRid;
	

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
	  scan = file.openScan();
	  this.file = file;
	  this.schema = schema;
	  lastRid = new RID();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  if(scan != null){
		  scan.close();
	  }
	  scan = file.openScan();	  
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  if( scan != null)
		  return true;
	  return false;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() { 
	  if(scan != null)
	  {
		  scan.close();	  
	  }
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(scan != null ){
		  return scan.hasNext();
	  }
	  return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  Tuple t = null;
	  if(scan != null){
		  byte[] array = scan.getNext(lastRid);
		  t = new Tuple(schema, array);	  
	  }
	  return t;
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
	  return lastRid;
  }

} // public class FileScan extends Iterator
