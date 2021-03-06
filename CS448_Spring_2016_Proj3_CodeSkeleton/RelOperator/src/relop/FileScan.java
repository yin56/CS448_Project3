package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;
import global.Page;
import global.PageId;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

	private HeapScan scan;	
	private boolean isOpen;
	private int indentDepth;
	private static RID mostRecentRID;
	private HeapFile file;
	private Tuple next;
	private boolean consumed;
	
	
  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
	  this.schema = schema;
	  this.file = file;
	  this.scan = file.openScan();
	  //this.next = new Tuple(schema);
	  //this.mostRecentRID = new RID();
	  isOpen = true;
	  indentDepth = 0;
	  consumed = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  super.indent(depth);
	  System.out.println("Filescan");
	  indentDepth += 1;
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  super.setSchema(schema);
	  scan = file.openScan();
	  consumed = true;
	  isOpen = true;
	  indentDepth = 0;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {	  
	  return isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  scan.close();
	  isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(!consumed){
		  return true;
	  }
	  if(scan.hasNext() == false){
		  //System.out.print("There are more recoreds to scan\n");
		  return false;
	  }
	  else{
		  //throw new IllegalStateException("No more tuples");
		  //System.out.print("No more recoreds to scan\n");
		  this.mostRecentRID = new RID();
		  scan.getNext(mostRecentRID);
		  //System.out.print("mosRecentRID2:" + mostRecentRID + "\n");
		  Tuple tuple = new Tuple(schema, file.selectRecord(mostRecentRID));
		  next = tuple;
		  
		  //mostRecentRID = rid;
		  //mostRecentRID.copyRID(rid);
		  
		  consumed = false;
		  return true;
		
	  }
	  
	  //return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  consumed = true;
	  return next;
	 
	 // 
	  /*
	  	    
	  if (hasNext() == false){
		  throw new IllegalStateException("No more tuples");
	  }	  
	  else{		 
		  RID rid = new RID();
		  scan.getNext(rid);
		  Tuple tuple = new Tuple(schema, file.selectRecord(rid));
		  		  
		  mostRecentRID = rid;
		  return tuple;
	  }*/
	  
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
	  //System.out.print("mosRecentRID:" + mostRecentRID + "\n");
	  return this.mostRecentRID;
  }
  
  public HeapFile returnHeapFile(){
	  return this.file;
  }

} // public class FileScan extends Iterator
