package tests;

// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {


	private static final String TEST_NAME = "query evaluation pipeline";

	public static void main(String args[])
	{
		QEPTest qepTest = new QEPTest();
		qepTest.create_minibase();

		// run all the test cases
		System.out.println("\n" + "Running " + TEST_NAME + "...");
		boolean status = PASS;
		status &= qepTest.test1();
		status &= qepTest.test2();
		status &= qepTest.test3();

		// display the final results
		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		} else {
			System.out.println("All " + TEST_NAME
					+ " completed; verify output for correctness.");
		}
	}

	protected boolean test1()
	{
		try{
		// that's all folks!
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

	protected boolean test2()
	{
		try{
		// that's all folks!
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

	protected boolean test3()
	{
		try{
		// that's all folks!
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
}
