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
	static int lcol;
	static int rcol;
	
	private boolean startJoin = true;
	private boolean consumed;
	Tuple leftTuple;
	private Tuple next;
	

	public HashJoin(Iterator left, Iterator right, int lcol, int rcol) {
		// TODO Auto-generated constructor stub
		this.outer = left;
		this.inner = right;
		this.lcol = lcol;
		this.rcol = rcol;
		this.schema = Schema.join(left.getSchema(), right.getSchema());
		consumed = true;

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
		consumed = true;
		
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
		
		if(!consumed){
			return false;
		}
		if(!outer.hasNext()){
			return true;
		}
		
		//System.out.print("lcol:" + lcol + "\n");
		//System.out.print("rcol:" + rcol + "\n");
		IndexScan innerScan;
		IndexScan outerScan;
		
		//HashIndex tempIndex = new HashIndex(null); 
		HashIndex indexInputInner = new HashIndex(null);
		HashIndex indexInputOuter = new HashIndex(null); 
		//HeapFile tempFile = new HeapFile(null);
		HeapFile fileInputInner = new HeapFile(null);
		HeapFile fileInputOuter = new HeapFile(null);
		int changeIn = 1;
		int changeOut = 1;
		
		
		//check if inner is FileScan
		//create 
		if(inner instanceof IndexScan){
			//do nothing,already have heapfile and indexfile 
			//innerScan = (IndexScan) inner;
			changeIn = 0;
		}
		else if(inner instanceof FileScan){
			//need to generate the indexfile from the FileScan
			System.out.print("inner is fileScan\n");
			HeapFile nHeap;
			nHeap = ((FileScan)inner).returnHeapFile();
			fileInputInner = nHeap;
			FileScan tempScan = new FileScan(inner.getSchema(), nHeap);

			while(inner.hasNext()){
				Tuple temp = inner.getNext(); //get the tuple from inner
				//temp.print();
				RID rec = new RID(); //new RID
				rec = tempScan.getLastRID(); //get the RID
				//System.out.print("Rec is " + rec + "\n");
				indexInputInner.insertEntry(new SearchKey(temp.getField(rcol)),rec); //finally get insert into indexFile needed
			}
		}
		else{
			while(inner.hasNext()){
				Tuple temp = inner.getNext();
				RID rid = fileInputInner.insertRecord(temp.getData()); //insert into tempHeapFile
				indexInputInner.insertEntry(new SearchKey(temp.getField(rcol)), rid); //insert intp tempIndexFile
			}
		}
		
		if(outer instanceof IndexScan){
			System.out.print("outer is fileScan\n");
			//do nothing,already have heapfile and indexfile
			//indexInputOuter = outer;
			//outerScan = (IndexScan) outer;
			changeOut = 0;
		}
		else if(outer instanceof FileScan){
			System.out.print("outer is fileScan\n");
			HeapFile nHeap;
			nHeap = ((FileScan)outer).returnHeapFile();
			fileInputOuter = nHeap;
			FileScan tempScan = new FileScan(outer.getSchema(), nHeap);
			while(outer.hasNext()){
				Tuple temp = outer.getNext(); //get the tuple from inner
				//temp.print();
				RID rec = new RID(); //new RID
				rec = tempScan.getLastRID(); //get the RID
				//System.out.print("key:" + temp.getField(lcol) + "\n");
				indexInputOuter.insertEntry(new SearchKey(temp.getField(lcol)),rec);//finally get insert into indexFile needed
			}
			
		}
		else{
			//int i = 0;
			while(outer.hasNext()){
				Tuple temp = outer.getNext();
				RID rid = fileInputOuter.insertRecord(temp.getData()); //insert into tempHeapFile
				indexInputOuter.insertEntry(new SearchKey(temp.getField(lcol)), rid); //insert intp tempIndexFile
				//i++;
			}
			
		}

		
		//construct IndexScans for probing
		if(changeIn == 1){
			System.out.print("In requires converting\n");
			innerScan = new IndexScan(inner.getSchema(), indexInputInner, fileInputInner);
		}
		else{
			innerScan = (IndexScan) inner;
		}
		if(changeOut == 1){
			System.out.print("Out requires converting\n");
			outerScan = new IndexScan(outer.getSchema(), indexInputOuter, fileInputOuter);
		}
		else{
			outerScan = (IndexScan) outer;
		}
		
		System.out.print("Finished converting\n");
		
		HashTableDup table = new HashTableDup();
		while(true){
			//probe
			while(outerScan.hasNext()){
				//System.out.print("Inserting prob\n");
				Tuple n = outerScan.getNext();
				//n.print();
				table.add(outerScan.getLastKey(), n);
			}
			while(innerScan.hasNext() == true){
				Tuple right = innerScan.getNext();
				//right.print();
				SearchKey k = innerScan.getLastKey();
				int hash = innerScan.getNextHash();
						
				Tuple[] t = table.getAll(k);
				
				
				if(t != null){
					System.out.print("Tuple is not NUll\n");
					//System.out.print("t length:" + t.length);
					for(int i = 0; i < t.length; i++){
						//t[i].print();
						//right.print();
						//if(t[i].getField(lcol).equals(right.getField(rcol))){
							Tuple result;	
							result = Tuple.join(t[i], right, schema);
							result.print();
							consumed = false;
							next = result;
							return true;
						//}
					}
				}
				else{
					System.out.print("Tuple is NUll\n");
				}
				//table.getAll(key);
			}
			table.clear();
			return false;
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
		
			
		//return false;
	}

	//@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		consumed = true;
		return next;
	}

	
}
