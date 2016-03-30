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

	private boolean startJoin = false;
	private boolean startJoin2 = false;
	private boolean consumed;
	private boolean finished = true;
	Tuple leftTuple;
	private Tuple next;
	// private HashIndex indexInputInner;
	// private HashIndex indexInputOuter;
	private IndexScan innerScan;
	private IndexScan outerScan;
	private IndexScan tempScan;
	int curHash;
	int nextHash;
	int curHash2;
	//int nextHash;
	int nextHash2;
	int count;
	//curHash =;
	//curHash2 = innerScan.getNextHash();
	
	HashTableDup table = new HashTableDup();

	public HashJoin(Iterator left, Iterator right, int lcol, int rcol) {
		// TODO Auto-generated constructor stub
		this.outer = left;
		this.inner = right;
		this.lcol = lcol;
		this.rcol = rcol;
		this.schema = Schema.join(left.getSchema(), right.getSchema());
		consumed = true;

		// IndexScan innerScan;
		// IndexScan outerScan;

		// HashIndex tempIndex = new HashIndex(null);
		HashIndex indexInputInner = new HashIndex(null);
		HashIndex indexInputOuter = new HashIndex(null);
		// HeapFile tempFile = new HeapFile(null);
		HeapFile fileInputInner = new HeapFile(null);
		HeapFile fileInputOuter = new HeapFile(null);
		int changeIn = 1;
		int changeOut = 1;

		// check if inner is FileScan
		// create
		if (inner instanceof IndexScan) {
			// do nothing,already have heapfile and indexfile
			// innerScan = (IndexScan) inner;
			changeIn = 0;
		} else if (inner instanceof FileScan) {
			// need to generate the indexfile from the FileScan
			//System.out.print("inner is fileScan\n");
			HeapFile nHeap;
			nHeap = ((FileScan) inner).returnHeapFile();
			fileInputInner = nHeap;
			FileScan tempScan = new FileScan(inner.getSchema(), nHeap);

			while (inner.hasNext()) {
				Tuple temp = inner.getNext(); // get the tuple from inner
				// temp.print();
				RID rec = new RID(); // new RID
				// rec = tempScan.getLastRID(); //get the RID
				rec = ((FileScan) inner).getLastRID();

				// System.out.print("Rec is " + rec + "\n");
				indexInputInner.insertEntry(new SearchKey(temp.getField(rcol)),
						rec); // finally get insert into indexFile needed
			}
		} else {
			while (inner.hasNext()) {
				Tuple temp = inner.getNext();
				RID rid = fileInputInner.insertRecord(temp.getData()); // insert
																		// into
																		// tempHeapFile
				indexInputInner.insertEntry(new SearchKey(temp.getField(rcol)),
						rid); // insert intp tempIndexFile
			}
		}

		if (outer instanceof IndexScan) {
			//System.out.print("outer is fileScan\n");
			// do nothing,already have heapfile and indexfile
			// indexInputOuter = outer;
			// outerScan = (IndexScan) outer;
			changeOut = 0;
		} else if (outer instanceof FileScan) {
			System.out.print("outer is fileScan\n");
			HeapFile nHeap;
			nHeap = ((FileScan) outer).returnHeapFile();
			fileInputOuter = nHeap;
			// FileScan tempScan = new FileScan(outer.getSchema(), nHeap);
			while (outer.hasNext()) {
				Tuple temp = outer.getNext(); // get the tuple from inner
				// temp.print();
				RID rec = new RID(); // new RID
				rec = ((FileScan) outer).getLastRID();
				// rec = tempScan.getLastRID(); //get the RID
				// System.out.print("key:" + temp.getField(lcol) + "\n");
				// System.out.print("Rec is " + rec + "\n");
				indexInputOuter.insertEntry(new SearchKey(temp.getField(lcol)),
						rec);// finally get insert into indexFile needed
			}

		} else {
			// int i = 0;
			while (outer.hasNext()) {
				Tuple temp = outer.getNext();
				RID rid = fileInputOuter.insertRecord(temp.getData()); // insert
																		// into
																		// tempHeapFile
				indexInputOuter.insertEntry(new SearchKey(temp.getField(lcol)),
						rid); // insert intp tempIndexFile
				// i++;
			}

		}

		// construct IndexScans for probing
		if (changeIn == 1) {
			//System.out.print("In requires converting\n");
			this.innerScan = new IndexScan(inner.getSchema(), indexInputInner,
					fileInputInner);
		} else {
			this.innerScan = (IndexScan) inner;
		}
		if (changeOut == 1) {
			//System.out.print("Out requires converting\n");
			this.outerScan = new IndexScan(outer.getSchema(), indexInputOuter,
					fileInputOuter);
		} else {
			this.outerScan = (IndexScan) outer;
		}
		curHash = outerScan.getNextHash();
		curHash2 = innerScan.getNextHash();

	}

	// @Override
	public void explain(int depth) {
		// TODO Auto-generated method stub
		System.out.print("HashJoin\n");
	}

	// @Override
	public void restart() {
		// TODO Auto-generated method stub
		outer.restart();
		consumed = true;

	}

	// @Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		if (outer.isOpen()) {
			return true;
		}
		return false;
	}

	// @Override
	public void close() {
		// TODO Auto-generated method stub
		outer.close();
		inner.close();

	}

	// @Override
	public boolean hasNext() {
		// TODO Auto-generated method stub

		if (!consumed) {
			return false;
		}

		/*
		  while(outerScan.hasNext()){ 
		 		outerScan.getNext().print();
		  		System.out.print("Hash: " + outerScan.getNextHash() + "\n"); 
		  		System.out.print("Hash: " + outerScan.getNextHash2() + "\n"); 
		  }
		 while(innerScan.hasNext()){ 
		 		innerScan.getNext().print();
		 		System.out.print("Hash: " + innerScan.getNextHash() + "\n");
		 		System.out.print("Hash: " + innerScan.getNextHash2() + "\n"); 
		
		 }
		 */
		 
		 
		 
		

		// System.out.print("Finished converting\n");

	
		tempScan = outerScan;

		

		// create HashTable
		while (true) {
			// curHash = outerScan.getNextHash();
			//if(outerScan.hasNext() == false){
			//	return false;
			//}
			if (finished == true) {
				finished = false;
				while (outerScan.hasNext()) {
					
					
					curHash = outerScan.getNextHash();
					

					 //add when nextone has not changed
					if (outerScan.getNextHash() == curHash) {
						//System.out.print("Inserting\n");
						Tuple temp = outerScan.getNext();
						// temp.print();
						table.add(new SearchKey(temp.getField(lcol)), temp);
					} 
					if (outerScan.getNextHash2() != curHash) {
						//System.out.print("Finished Table Insertion\n");
						curHash = outerScan.getNextHash2();
						nextHash = outerScan.getNextHash2();
						 //System.out.print("curHash:" + curHash + "\n");
						 break;
					}
					
				}
			}

			// get matching tuples
			while (innerScan.hasNext()) {
				//System.out.print("Finding matches\n");
				
				
				//if (startJoin2 == false) {
				if(curHash2 != innerScan.getNextHash()){
					//System.out.print("nextHash will be different\n");
					curHash2 = innerScan.getNextHash();
				}
				
				
				
				// if has not reach limit yet keep adding
				if (curHash2 == innerScan.getNextHash()) {
					//System.out.print("table size:" + table.size() + "\n");
					//System.out.print("Within Partition\n");
					Tuple intup = innerScan.getNext();
					 
					SearchKey tempKey = new SearchKey(intup.getField(rcol));
					
					Tuple[] t = table.getAll(tempKey);

					if (t != null) {												
						
						for (int i = 0; i < t.length; i++) {
							if (intup.getField(rcol) == t[i].getField(lcol)) {
								// System.out.print("found match\n");
								// t[i].print();
								consumed = true;
								next = Tuple.join(t[i], intup, this.schema);
								
								return true;
							}
						}
						// break;
					} else {
						 //System.out.print("t is null\n");
					}
				}
				if (innerScan.getNextHash2() == nextHash) {
					//System.out.print("Finished Matching\n");
					curHash2 = innerScan.getNextHash2();
					//System.out.print("curHash2after:" + curHash + "\n");
					finished = true;
					//return false;
					break;
				} 
			}
			//System.out.print("Restarting\n");
			table.clear();
			// innerScan.restart();
			if (outerScan.hasNext() == false) {
				//System.out.print("Outer is over\n");
				return false;
			}
			//count++;
			//if(count == 7){
			//	return false;
			//}
			//return false;
			// curHash = outerScan.getNextHash();
			return false;

		}
		

		
	}

	// @Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		consumed = true;
		return next;
	}

}
