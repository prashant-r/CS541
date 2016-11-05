package relop;

import java.util.*;
import global.*;
import heap.*;
import index.*;

// Taken from :

// Hash Join algorithm used : 

//https://rosettacode.org/wiki/Hash_join
// let A = the first input table (or ideally, the larger one)
// let B = the second input table (or ideally, the smaller one)
// let jA = the join column ID of table A
// let jB = the join column ID of table B
// let MB = a multimap for mapping from single values to multiple rows of table B (starts out empty)
// let C = the output table (starts out empty)

// for each row b in table B:
//   place b in multimap MB under key b(jB)

//for each row a in table A:
//   for each row b in multimap MB under key a(jA):
//      let c = the concatenation of row a and row b
//      place row c in table C


public class HashJoin extends Iterator {

	private int jA;
	private int jB;
	private IndexScan A;
	private IndexScan B;
	private HashTableDup multimap;
	private List<Tuple> nextTuples;
	private int nextPosition;
	private Tuple currentTuple;

	public HashJoin(Iterator itA, Iterator itB, Integer jA, Integer jB)
	{
		schema = Schema.join(itA.schema, itB.schema);
		this.jA = jA;
		this.jB = jB;
		multimap = new HashTableDup();
		A = getIndexScanFromIteratorAndColumn(itA, jA);
		B = getIndexScanFromIteratorAndColumn(itB, jB);

		nextPosition = 0;
		nextTuples = null;
		currentTuple = null;
		// create the hash table for the inner table B
		while (B.hasNext()) {
			Tuple b = B.getNext();
			SearchKey key = new SearchKey(b.getField(jB).toString());
			multimap.add(key, b);
		}
		B.close();
	}

	private IndexScan getIndexScanFromIteratorAndColumn(Iterator iterator, Integer column)
	{
		if(iterator instanceof IndexScan)
		{
			return (IndexScan) iterator;
		}
		else if(iterator instanceof FileScan)
		{
			// temporary HashIndex
			HashIndex hashIndex = new HashIndex(null);

			// Case the FileScan
			FileScan fsCast = (FileScan) iterator;
			// create the temporary HashIndex
			while(fsCast.hasNext())
			{
				Tuple tuple = fsCast.getNext();
				hashIndex.insertEntry(new SearchKey(tuple.getField(column).toString()), fsCast.getLastRID());
			}
			return new IndexScan(fsCast.getSchema(), hashIndex, fsCast.getHeapFile());
		} 
		else
		{
			// create the heapFile
			HeapFile heapFile = new HeapFile(null);
			while(iterator.hasNext())
			{
				heapFile.insertRecord(iterator.getNext().data);
			}
			FileScan materialized = new FileScan(iterator.getSchema(), heapFile);
			return getIndexScanFromIteratorAndColumn(materialized,column);
		}
	}


	public boolean isOpen() {
		return A.isOpen();
	}

	public void close() {
		A.close();
		if(B.isOpen()) B.close();
		multimap = null;
		nextTuples = null;
		nextPosition = 0;
		currentTuple = null;
		jA = 0;
		jB = 0;
	}
	public boolean hasNext() {
		currentTuple = prefetchTuple();
		return currentTuple != null;
	}

	public Tuple prefetchTuple()
	{
		if (nextTuples != null) {
			Tuple prefetched = nextTuples.get(nextPosition++);
			if (nextPosition == nextTuples.size()) {
				nextTuples = null;
				nextPosition = 0;
			}
			return prefetched;
		}
		
		while (this.A.hasNext()) {
			Tuple tupleA = this.A.getNext();
			SearchKey key = new SearchKey(tupleA.getField(this.jA).toString());
			if (multimap.containsKey(key)) {
				List<Tuple> tupleMatches = Arrays.asList(multimap.getAll(key));
				nextTuples = new ArrayList<Tuple>();
				for (Tuple tupleMatch : tupleMatches) {
					nextTuples.add(Tuple.join(tupleA, tupleMatch, schema));
				}
				return prefetchTuple();
			}
		}
		return null;
	}

	public Tuple getNext() {
		if(currentTuple == null) throw new IllegalStateException("HashJoin getNext() failed. ");
		return currentTuple;
	}

	public void restart() {
		A.restart();
		nextTuples = null;
		nextPosition = 0;
		currentTuple = null;
	}

	public void explain(int depth) {
		indent(depth);
		System.out.println("HashJoin Iterator: ");
		A.explain(depth + 1);
		B.explain(depth + 1);
	}
}
