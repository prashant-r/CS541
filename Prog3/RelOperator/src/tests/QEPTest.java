package tests;

import java.io.*;
import java.util.*;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;


// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {


	private static final String TEST_NAME = "Query Evaluation Pipeline";

	/** Size of tables in test3. */
	private static final int SUPER_SIZE = 2000;

	/** Department table schema. */
	private static Schema s_dep;

	/** Employee table schema. */
	private static Schema s_employees;


	private static HeapFile eHeapFile;
	private static HeapFile dHeapFile;

	//Entities
	//Employee (EmpId, Name, Age, Salary, DeptID), and 
	//●
	//Department (DeptId, Name, MinSalary, MaxSalary)

	public static void main(String args[])
	{
		QEPTest qepTest = new QEPTest();
		qepTest.create_minibase();

		// initialize schema for the "Employees" table
		s_employees = new Schema(5);
		s_employees.initField(0, AttrType.INTEGER, 4, "EmpId");
		s_employees.initField(1, AttrType.STRING, 20, "Name");
		s_employees.initField(2, AttrType.INTEGER, 4, "Age");
		s_employees.initField(3, AttrType.INTEGER,10, "Salary");
		s_employees.initField(4, AttrType.INTEGER, 4, "DeptID");

		// initialize schema for the "Departments" table
		s_dep = new Schema(4);
		s_dep.initField(0, AttrType.INTEGER, 4, "DeptId");
		s_dep.initField(1, AttrType.STRING, 20, "Name");
		s_dep.initField(2, AttrType.INTEGER, 10, "MinSalary");
		s_dep.initField(3, AttrType.INTEGER, 10, "MaxSalary");

		boolean status = PASS;
		// load the data


		HeapFile empHeapFile = new HeapFile("EmpHeapFile");
		HeapFile deptHeapFile = new HeapFile("DepartmentHeapFile");

		File deptFile = new File("src/tests/SampleData/Department.txt");
		File empFile = new File("src/tests/SampleData/Employee.txt");

		try{
			Scanner dis=new Scanner(deptFile);
			dis.nextLine();

			while(dis.hasNextLine())
			{
				int a,c,d;
				String b;
				String line;
				String[] lineVector;
        	line = dis.nextLine(); //read 1,2,3
        	//separate all values by comma
        	lineVector = line.split(",");

	        //parsing the values to Integer
        	a=Integer.parseInt(lineVector[0].trim());
        	b=lineVector[1];
        	c=Integer.parseInt(lineVector[2].trim());
        	d =Integer.parseInt(lineVector[3].trim());

        	//System.out.println("a " + a + " b " + b  + " c " + c + " d "+ d );

        	Tuple tuple = new Tuple(s_dep);
        	tuple.setAllFields(a, b, c, d);
        	tuple.insertIntoFile(deptHeapFile);
        }

    }
    catch(Exception e)
    {
    	e.printStackTrace();
    	status &= false;
    }

    try{
    	Scanner eis=new Scanner(empFile);
    	eis.nextLine();

    	while(eis.hasNextLine())
    	{
    		int a,c,d,e;
    		String b;
    		String line;
    		String[] lineVector;
        	line = eis.nextLine(); //read 1,2,3
        	//separate all values by comma
        	lineVector = line.split(",");
	        //parsing the values to Integer
        	a=Integer.parseInt(lineVector[0].trim());
        	b=lineVector[1];
        	c=Integer.parseInt(lineVector[2].trim());
        	d =Integer.parseInt(lineVector[3].trim());
        	e =Integer.parseInt(lineVector[4].trim());

        	//System.out.println("a " + a + " b " + b  + " c " + c + " d "+ d  + " e " + e);
        	Tuple tuple = new Tuple(s_employees);
        	tuple.setAllFields(a, b, c, d, e);
        	tuple.insertIntoFile(empHeapFile);
        }
    }
    catch(Exception e)
    {
    	e.printStackTrace();
    	status &= false;
    }

    dHeapFile = deptHeapFile;
    eHeapFile = empHeapFile;

		// run all the test cases
    System.out.println("\n" + "Running " + TEST_NAME + "...");

    status &= qepTest.test1();
    status &= qepTest.test2();
    status &= qepTest.test3();
	status &= qepTest.test4();


		// display the final results
    System.out.println();
    if (status != PASS) {
    	System.out.println("Error(s) encountered during " + TEST_NAME + ".");
    } else {
    	System.out.println("All " + TEST_NAME
    		+ " completed; verify output for correctness.");
    }
}


//Display for each employee his ID, Name and Age
 
//SQL translation : Select EmpId, Name, Age FROM Employee
protected boolean test1()
{
	try{
		// that's all folks!
		Projection pro = new Projection(new FileScan(s_employees,eHeapFile), 0, 1, 2);
		pro.execute();
		System.out.print("\n\nTest 1 completed without exception.");
		return PASS;
	}
	catch(Exception e)
	{
		e.printStackTrace(System.out);
		System.out.print("\n\nTest 1 terminated because of exception.");
		return FAIL;
	}
	finally
	{
		System.out.println();
	}
}

	//Display the Name for the departments with MinSalary = MaxSalary

	//SQL translation : Select Name FROM Department where MinSalary = MaxSalary
protected boolean test2()
{
	try{


		// that's all folks!
		
		Predicate predicate = new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 3, AttrType.FIELDNO, 2);
		
		Predicate predicates[] = new Predicate[1];

		predicates[0] = predicate;

		Selection sel = new Selection(new FileScan(s_dep,dHeapFile), predicates);

		Projection pro = new Projection(sel, 1);

		pro.execute();

		System.out.print("\n\nTest 2 completed without exception.");
		return PASS;
	}
	catch(Exception e)
	{
		e.printStackTrace(System.out);
		System.out.print("\n\nTest 2 terminated because of exception.");
		return FAIL;
	}
	finally
	{
		System.out.println();
	}
}

	// For each employee, display his Name and the Name of his department as well as the maximum salary of his department

	// SQL translation: 
	// Select e.Name, d.Name, d.MaxSalary from Employee e join Department d on d.deptID = e.deptID

protected boolean test3()
{
	try{
		// that's all folks!

		HashJoin hashJoin = new HashJoin(new FileScan(s_employees,eHeapFile),new FileScan(s_dep,dHeapFile), 4,
					0);
		Projection pro = new Projection(hashJoin, 1, 6, 8);
		pro.execute();

		System.out.print("\n\nTest 3 completed without exception.");
		return PASS;
	}
	catch(Exception e)
	{
		e.printStackTrace(System.out);
		System.out.print("\n\nTest 3 terminated because of exception.");
		return FAIL;
	}
	finally
	{
		System.out.println();
	}
}

	//Display the Name for each employee whose Salary is greater than the maximum salary 
	//of his department.


	// SQL translation : Select e.Name FROM Employee e join Department d on e.deptID = d.deptID where
	// e.Salary > d.MaxSalary
protected boolean test4()
{
	try{
		// that's all folks!
		HashJoin hashJoin = new HashJoin(new FileScan(s_employees,eHeapFile),new FileScan(s_dep,dHeapFile), 4,
					0);
		Predicate predicate = new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FIELDNO, 8);
		
		Predicate predicates[] = new Predicate[1];

		predicates[0] = predicate;
		Selection selection = new Selection(hashJoin, predicates);
		Projection projection = new Projection(selection, 1, 6, 8);
		projection.execute();



		System.out.print("\n\nTest 4 completed without exception.");
		return PASS;
	}
	catch(Exception e)
	{
		e.printStackTrace(System.out);
		System.out.print("\n\nTest 4 terminated because of exception.");
		return FAIL;
	}
	finally
	{
		System.out.println();
	}
}
}
