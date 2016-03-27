package relop;

import java.util.Arrays;

import global.RID;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

	private int indentDepth;
	private Predicate [] preds;
	private Schema schema;
	private Iterator iter;

	
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
	
	//iter is what will be remove tuples from
	
  public Selection(Iterator iter, Predicate... preds) {
	  this.preds = preds;
	  this.iter = iter;
	  schema = iter.getSchema();
	  indentDepth = 0;

  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  super.indent(depth);
	  System.out.println("Selection");
	  indentDepth += 1;
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  iter.restart();
	  indentDepth = 0;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
   	return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  
	  Tuple tuple = null;
	  boolean haveMatchingTuple = false;
	  
	  while(haveMatchingTuple == false){
	  
	  if (hasNext() == false){
		  throw new IllegalStateException("No more tuples");
	  }	  
	  else{		 	
		  tuple = iter.getNext();
		  boolean doesTupleMatch = true;
		  for (int i = 0; i < preds.length; i++){
			  Predicate tempPredicate = preds[i];
			  if (tempPredicate.validate(schema)){
				  if (tempPredicate.evaluate(tuple) == false){
					  doesTupleMatch = false;
				  }
			  }
		  }//for
		  if (doesTupleMatch){
			  haveMatchingTuple = true;
		  }
	   }//else
	 }//while 
	  return tuple;
  }

} // public class Selection extends Iterator
