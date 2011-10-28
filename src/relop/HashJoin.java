package relop;

import global.RID;
import global.SearchKey;
import index.BucketScan;
import index.HashIndex;

/**
 * Implements the hash-based join algorithm described in section 14.4.3 of the
 * textbook (3rd edition; see pages 463 to 464). HashIndex is used to partition
 * the tuples into buckets, and HashTableDup is used to store a partition in
 * memory during the matching phase.
 */
public class HashJoin extends Iterator {
	
	private Iterator liter;
	private Iterator riter;
	private Integer lcol;
	private Integer rcol;
	private HashIndex left;
	private HashIndex right;

  /**
   * Constructs a hash join, given the left and right iterators and which
   * columns to match (relative to their individual schemas).
   */
  public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
    liter = left;
    riter = right;
    this.lcol = lcol;
    this.rcol = rcol;
    this.left = new HashIndex("leftpartition");
    this.right = new HashIndex("rightpartition");
    
    // create partitions, put R tuples in left Hashindex etc.
    while (liter.hasNext()){
    	Tuple t = liter.getNext();
    	RID r = ((FileScan)liter).getLastRID();
    	this.left.insertEntry(new SearchKey(t.getField(lcol)), r);
    	
    }
    while (riter.hasNext()){
    	Tuple t = riter.getNext();
    	RID r = ((FileScan)riter).getLastRID();
    	this.right.insertEntry(new SearchKey(t.getField(rcol)), r);
    	
    }
    
    //verify();
    
  }

/*  public void verify(){
	  if (left instanceof FileScan)
	  BucketScan b = left.openScan();
	  while (b.hasNext()){
		  System.out.println (b.getNext())
	  }
  }*/
  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    //TODO: what to do??
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //TODO: will decide later
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    // TODO: 
	  return true;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    // TODO:
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    // TODO: 
	  return true;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    return null;
  }

} // public class HashJoin extends Iterator
