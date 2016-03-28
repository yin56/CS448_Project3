package relop;

import global.RID;
import heap.HeapFile;
import index.HashIndex;

public class HashJoin extends Iterator {
	
	private Iterator outer;
	private Iterator inner;
	int lcol;
	int rcol;
	
	private boolean startJoin = true;
	private boolean nextTupleConsumed;
	Tuple leftTuple;
	private Tuple next;
	

	public HashJoin(Iterator left, Iterator right, int lcol, int rcol) {
		// TODO Auto-generated constructor stub
		this.outer = left;
		this.inner = right;
		this.lcol = lcol;
		this.rcol = rcol;
		this.schema = Schema.join(left.getSchema(), right.getSchema());
	}


	//@Override
	public void explain(int depth) {
		// TODO Auto-generated method stub
		System.out.print("HashJoin\n");
	}

	//@Override
	public void restart() {
		// TODO Auto-generated method stub
		outer.restart();
		nextTupleConsumed = true;
		
	}

	//@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		if(outer.isOpen()){
			return true;
		}
		return false;
	}

	//@Override
	public void close() {
		// TODO Auto-generated method stub
		outer.close();
		inner.close();
		
	}

	//@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		
		//right now im trying to figure how to convert all the non-indexscan iterators
		//into indexscans, then we need use indexscan to put them into buckets
		//not sure about the stuff after that, but we need to follow the hashjoin algorithm in the book
		
		HashIndex tempIndex = new HashIndex(null); 
		HeapFile tempFile = new HeapFile(null);
		//KeyScan keyScan = new KeyScan(schema, tempIndex, null, tempFile);
		//Projection proj = new Projection(inner, 1);
		FileScan exfile = new FileScan(schema, tempFile) ;
		
		//Selection select = new Selection(inner, null);

		//dexScan indexScan = new IndexScan(schema, tempIndex, tempFile);
		
		if(inner.getClass().equals(exfile.getClass())){
			System.out.print("Both are fileScans\n");
			HeapFile nHeap;
			nHeap = ((FileScan)inner).getHeapFile();
			RID rec = new RID();
			nHeap.selectRecord(rec);
			
		
			
			//convert inner fileScan to IndexScan
		
	
			
		}
		//indexScan.
		
		//convert filescan to indexscan
		//System.out.print(outer.getClass().getName());
		
		
		
		if(!nextTupleConsumed){
			return true;
		}
	
		if(!outer.hasNext()){
			return false;
		}
		
		Tuple rightTuple;
		
		if (startJoin){
			leftTuple = outer.getNext();
			startJoin = false;
		}
		
		
		//leftTuple.
		while(outer.hasNext()){
		//	temp.insertEntry(arg0, arg1);
		}
			
		return false;
	}

	//@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		return next;
	}

	
}
