	package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	
	private Integer field[];
	private Iterator it;
	private Tuple next;
	private boolean start = true;
	private boolean consumed;
	private Tuple proj;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
	  System.out.print("Constructing Projection\n");
	  this.schema = iter.getSchema();
	  this.field = fields;
	  this.it = iter;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.print("Projection\n");
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  it.restart();
	  consumed = true;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  if(it.isOpen() == true){
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
	  it.close();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  //next.s
	 proj = new Tuple(schema);
	  //result.print();
	  while(it.hasNext()){
		  next = it.getNext();
		  for(int i = 0; i < field.length; i++){
			  //System.out.print(next.getField(field[i]).getClass().toString() +"\n");
			 proj.setField(field[i], next.getField(field[i])); 
		  }
		  //proj = result;
		  consumed = true;
		  return true;
	  }
	  return false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  return proj;
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class Projection extends Iterator
