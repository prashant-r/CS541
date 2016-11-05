package relop;
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

	IndexScan jAScan;
	IndexScan jBScan;

	HashTableDup multimap = new HashTableDup();

	Tuple currentTuple;


	public HashJoin(Iterator A)
	{

	}

	public HashJoin(Iterator A, Iterator B, Integer jA, Integer jB)
	{
		schema = Schema.join(A.schema, B.schema);
		this.jA = jA;
		this.jB = jB;

		this.leftScan = getIndexScanFromIteratorAndColumn(A, jA);
		this.rightScan = getIndexScanFromIteratorAndColumn(B, jB);
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
				hashIndex.insertEntry(new SearchKey((String) tuple.getField(column)), fsCast.getLastRID());
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
    	return jAScan.isOpen() && jBScan.isOpen();
  	}

	public void close() {
    	jAScan.close();
    	jBScan.close();
    	
  	}
	public boolean hasNext() {
    	return false;
  	}

	public Tuple getNext() {
    	return null;
  	}

  	public void restart() {



  	}

  	public void explain(int depth) {
  		indent(depth);
  		System.out.println("HashJoin Iterator: ");
  		jAScan.explain(depth + 1);
  		jBScan.explain(depth + 1);
  	}
}
