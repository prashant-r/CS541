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

	int jA;
	int jB;
	IndexScan A;
	IndexScan B;
	HashTableDup multimap;
	private List<Tuple> nextTuples;
	private int nextPosition;

	public HashJoin(Iterator itA, Iterator itB, Integer jA, Integer jB)
	{
		schema = Schema.join(itA.schema, itB.schema);
		this.jA = jA;
		this.jB = jB;
		multimap = new HashTableDup();
		A = getIndexScanFromIteratorAndColumn(itA, jA);
		B = getIndexScanFromIteratorAndColumn(itB, jB);

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
		multimap = null;
		nextTuples = null;
		nextPosition = 0;
		jA = 0;
		jB = 0;
	}
	public boolean hasNext() {
		if (nextTuples != null) {
			Tuple result = nextTuples.get(nextPosition++);
			if (nextPosition == nextTuples.size()) {
				nextTuples = null;
				nextPosition = 0;
				return hasNext();
			}
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
				nextPosition = 0;
				return true;
			}
		}
		return false;
	}

	public Tuple getNext() {
		if(nextTuples == null) throw new IllegalStateException("HashJoin getNext() failed. ");
		return nextTuples.get(nextPosition);
	}

	public void restart() {
		A.restart();
		if (nextTuples != null)
			nextTuples.clear();
		multimap = new HashTableDup();
		nextPosition = 0;
	}

	public void explain(int depth) {
		indent(depth);
		System.out.println("HashJoin Iterator: ");
		A.explain(depth + 1);
		B.explain(depth + 1);
	}
}
