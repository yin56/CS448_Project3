package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
	
	//Schema schema;
	private HashIndex index;
	private SearchKey key;
	private HeapFile file;
	private HashScan hashscan;
	private boolean open = true;
	private Tuple next;
	//private boolean consumed;	

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
	  System.out.print("Creating a keyScan\n");
	 this.schema = schema;
	 this.index = index;
	 this.key = key;
	 this.file = file;
	 this.hashscan = index.openScan(key);
	 //System.out.print(hashscan.getClass().getName() + "\n");
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.print("KeyScan\n");
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  if(isOpen() == true){
		  close();
		  hashscan = index.openScan(key);
		  open = true;
	  }
	  else{
		  hashscan = index.openScan(key);
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
	  hashscan.close();
	  open = false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  System.out.print("KeyScan hasNext\n");
	  if(hashscan.hasNext()){
		  RID record = new RID();
		  record = hashscan.getNext();
		  //file.selectRecord(record);
		  Tuple tup = new Tuple(schema, file.selectRecord(record));
		  //index.deleteEntry(key, record);
		  next = tup;
		  return true;
	  }
	  else{
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
	  return next;
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class KeyScan extends Iterator
