package relop;

import java.io.File;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

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
		IndexScan innerScan;
		IndexScan outerScan;
		
		//HashIndex tempIndex = new HashIndex(null); 
		HashIndex indexInputInner = new HashIndex(null);
		HashIndex indexInputOuter = new HashIndex(null); 
		//HeapFile tempFile = new HeapFile(null);
		HeapFile fileInputInner = new HeapFile(null);
		HeapFile fileInputOuter = new HeapFile(null);
		
		
		//check if inner is FileScan
		//create 
		if(inner instanceof IndexScan){
			//do nothing,already have heapfile and indexfile 
			innerScan = (IndexScan) inner;
		}
		else if(inner instanceof FileScan){
			//need to generate the indexfile from the FileScan
			System.out.print("inner is fileScan\n");
			HeapFile nHeap;
			nHeap = ((FileScan)inner).returnHeapFile();
			fileInputInner = nHeap;
			FileScan tempScan = new FileScan(schema, nHeap);
			int i = 0;
			while(inner.hasNext()){
				inner.getNext(); //get the tuple from inner
				RID rec = new RID(); //new RID
				rec = tempScan.getLastRID(); //get the RID
				System.out.print("Rec is " + rec + "\n");
				indexInputInner.insertEntry(new SearchKey(i),rec); //finally get insert into indexFile needed
				i++;
			}
		}
		else{
			int i = 0;
			while(inner.hasNext()){
				Tuple next = inner.getNext();
				RID rid = fileInputInner.insertRecord(next.getData()); //insert into tempHeapFile
				indexInputInner.insertEntry(new SearchKey(i), rid); //insert intp tempIndexFile
				i++;
			}
		}
		
		if(outer instanceof IndexScan){
			//do nothing,already have heapfile and indexfile
			//indexInputOuter = outer;
			outerScan = (IndexScan) outer;
		}
		else if(outer instanceof FileScan){
			System.out.print("outer is fileScan\n");
			HeapFile nHeap;
			nHeap = ((FileScan)outer).returnHeapFile();
			fileInputOuter = nHeap;
			FileScan tempScan = new FileScan(schema, nHeap);
			int i = 0;
			while(outer.hasNext()){
				outer.getNext(); //get the tuple from inner
				RID rec = new RID(); //new RID
				rec = tempScan.getLastRID(); //get the RID
				indexInputInner.insertEntry(new SearchKey(i),rec); //finally get insert into indexFile needed
				i++;
			}
			
		}
		else{
			int i = 0;
			while(outer.hasNext()){
				Tuple next = outer.getNext();
				RID rid = fileInputOuter.insertRecord(next.getData()); //insert into tempHeapFile
				indexInputOuter.insertEntry(new SearchKey(i), rid); //insert intp tempIndexFile
				i++;
			}
			
		}
		
		//construct IndexScans for probing
		innerScan = new IndexScan(schema, indexInputInner, fileInputInner);
		outerScan = new IndexScan(schema, indexInputOuter, fileInputOuter);
		
		System.out.print("Finished converting\n");
		
		HashTableDup table = new HashTableDup();
		while(innerScan.hasNext() && outerScan.hasNext()){
			//probe
			int i = 0;
			while(inner.hasNext()){
				table.add(new SearchKey(i), inner.getNext());
				i++;
			}
			int j = 0;
			while(outer.hasNext()){
				//table.getAll(key);
			}
			//while(t)
		}
		
		
		
		
		//after all conversion, create the indexscan
		//IndexScan  scan1 = new In
		
		/*
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
		
		*/
		
		//leftTuple.
		
			
		return false;
	}

	//@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		return next;
	}

	
}
