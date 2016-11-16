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


// More docs info : http://research.cs.wisc.edu/coral/mini_doc/intro/single_user.html

public class HashJoin extends Iterator {

	private int jA;
	private int jB;
	private IndexScan A;
	private IndexScan B;
	private HashTableDup multimap;
	private Tuple nextTuple = null;
	private Tuple[] nextTuples;
	private int currenthashval = -1;
	private int nextPosition;
	private Tuple currentTuple;

	public HashJoin(Iterator itA, Iterator itB, Integer jA, Integer jB) {
		schema = Schema.join(itA.schema, itB.schema);
		this.jA = jA;
		this.jB = jB;
		multimap = new HashTableDup();

		nextPosition = 0;
		nextTuples = null;
		currentTuple = null;

		if (itA instanceof IndexScan)
			this.A = (IndexScan) itA;

		this.A= getIndexScanFromIteratorAndColumn(itA,jA);

		if (itB instanceof IndexScan)
			this.B = (IndexScan) itB;

		this.B= getIndexScanFromIteratorAndColumn(itB,jB);

	}

	private IndexScan getIndexScanFromIteratorAndColumn(Iterator iterator, Integer column)
	{
		return (iterator instanceof FileScan) ?
				getIndexScanFromFileScan((FileScan) iterator, column, ((FileScan) iterator).getHeapFile()) :
				getIndexScan(iterator, column);

	}

	public IndexScan getIndexScanFromFileScan(FileScan fs, Integer field, HeapFile hpfile)
	{
		HashIndex hi = new HashIndex(null);
		while(fs.hasNext()){
			Tuple t = fs.getNext();
			hi.insertEntry(new SearchKey(t.getField(field).toString()), fs.getLastRID());
		}
		IndexScan is = new IndexScan(fs.getSchema(),hi,hpfile);
		return is;
	}

	public IndexScan getIndexScan(Iterator it, Integer field)
	{
		HeapFile hf = new HeapFile(null);
		while(it.hasNext()){
			hf.insertRecord(it.getNext().data);
		}
		FileScan temp = new FileScan(it.schema, hf);
		return getIndexScanFromFileScan(temp, field, hf);
	}

	@Override
	public void explain(int depth) {
		indent(depth);
		System.out.println("HashJoin Iterator: ");
		A.explain(depth + 1);
		B.explain(depth + 1);

	}

	@Override
	public void restart() {
		// TODO Auto-generated method stub
		multimap = null;
		A.restart();
		B.restart();
		nextTuple = null;
		nextTuples = null;
		currenthashval = (Integer) null;
		currentTuple = null;
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		if(A.isOpen()){
			if(B.isOpen()){
				return true;
			}
		}
		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		A.close();
		B.close();
		nextTuples = null;
		nextPosition = 0;
		currentTuple = null;
		nextTuple = null;
		multimap = null;
	}

	public void constructMultiMap(int hashval)
	{
		A.restart();
		multimap.clear();
		while(A.hasNext())
		{
			int temp = A.getNextHash();
			if(temp == hashval)
			{
				Tuple leftTuple = A.getNext();
				multimap.add(new SearchKey(leftTuple.getField(jA).toString()), leftTuple);
			}
			else
			{
				A.getNext();
			}
		}
	}

	@Override
	public boolean hasNext()
	{
		// TODO Auto-generated method stub

		if(nextTuples == null)
		{
			if (!B.hasNext()) return false;

			int nexthashval = B.getNextHash();
			currentTuple = B.getNext();

			if (nexthashval != currenthashval)
			{
				currenthashval = nexthashval;
				constructMultiMap(currenthashval);
			}
			nextTuples = multimap.getAll(new SearchKey(currentTuple.getField(jB).toString()));
			return CheckAndJoinTuples();
		}
		else
		{
			if(nextPosition == nextTuples.length)
			{
				nextTuples = null;
				nextPosition = 0;
				return hasNext();
			}
			else
				return TryAndJoinTuples();

		}
	}

	private boolean CheckAndJoinTuples()
	{
		nextPosition = 0;
		if (nextTuples == null)
			return hasNext();

		if(nextPosition == nextTuples.length)
		{
			nextTuples = null;
			return hasNext();
		}

		return TryAndJoinTuples();
	}

	private boolean TryAndJoinTuples()
	{
		while (nextPosition <= nextTuples.length - 1)
		{
			if (currentTuple.getField(jB).equals(nextTuples[nextPosition].getField(jA)))
			{
				nextTuple = Tuple.join(nextTuples[nextPosition], currentTuple, schema);
				nextPosition++;
				return true;
			}
			else
			{
				nextPosition++;
			}
		}
		nextPosition = 0;
		nextTuples = null;
		return hasNext();
	}

	@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		if(nextTuple == null) throw new IllegalStateException("HashJoin getNext() failed. ");
		return nextTuple;

	}
}
