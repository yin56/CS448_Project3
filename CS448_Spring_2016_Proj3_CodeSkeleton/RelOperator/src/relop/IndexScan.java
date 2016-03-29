
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
	private static SearchKey key;
	boolean consumed;
	static int hash;
	Tuple next;
	boolean noMore = false;
	static int numBuckets = 0;

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
	  
	  super.setSchema(schema);
	  scan = index.openScan();
	  consumed = true;
	  open = true;
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
	  //System.out.print("indexScan hasNext\n");
	  
	  if(!consumed){
		  return true;
	  }
	  if(scan.hasNext() == false){
		  System.out.print("false\n");
		  consumed = true;
		  return false;
	  }
	  else{
		  //System.out.print("true\n");
		  RID record = new RID();
		  record = scan.getNext();
		  Tuple tup = new Tuple(schema, file.selectRecord(record));
		  //tup.print();
		  hash = scan.getNextHash();
		  key = scan.getLastKey();
		  //System.out.print("key is " + key + "\n");
		  numBuckets++;
		  next = tup;
		  //System.out.print(tup.getField(0) + "  " +  tup.getField(1) + "\n");
		  consumed = false;
		  return true;
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	consumed = true;
	return next;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  //System.out.print("key2 is " + key.toString() + "\n");
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
