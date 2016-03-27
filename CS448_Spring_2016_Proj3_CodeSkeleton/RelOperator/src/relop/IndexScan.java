
package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
	
	HashIndex index;
	HeapFile file;
	boolean open = false;
	private BucketScan scan;
	private SearchKey key;
	boolean consumed;
	int hash;
	Tuple next;
	boolean noMore = false;
	int numBuckets = 0;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  this.schema = schema;
	  this.index = index;
	  this.file = file;
	  this.scan = index.openScan();
	  consumed = true;
	  
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.print("IndexScan\n");
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  if(isOpen() == true){
		  close();
		  scan = index.openScan();
		  open = true;
	  }
	  else{
		  scan = index.openScan();
		  open = true;
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  if(open == true){
		  return true;
	  }
	  else{
		  return false;
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  scan.close();
	  open = false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(scan.hasNext() == true){
		  noMore = false;
		  RID record = new RID();
		  record = scan.getNext();
		  //file.selectRecord(record);
		  Tuple tup = new Tuple(schema, file.selectRecord(record));
		  hash = scan.getNextHash();
		  //hash = nhash;
		  key = scan.getLastKey();
		  numBuckets++;
		  next = tup;
		  return true;
	  }
	  else{
		  noMore = true;
		  return false;
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if(hasNext() == true){
		 consumed = true;
		 return next;
	  }
	  else{
		  throw new IllegalStateException("NO MORE TUPLES");
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  return  key;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  if(noMore == false){
		  return hash;
	  }
	  else{
		  return numBuckets;
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class IndexScan extends Iterator
