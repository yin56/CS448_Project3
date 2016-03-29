package tests;

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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class QEPTest extends TestDriver {
	/** The display name of the test suite. */
	private static final String TEST_NAME = "query evaluation pipeline tests";

	/** Employee table schema. */
	private static Schema employeeSchema;

	/** Department table schema. */
	private static Schema departmentSchema;
	
	//heapfile for employee table
	private static HeapFile employeeHeapFile;
	
	//heapfile for department table
	private static HeapFile departmentHeapFile;
	
	// --------------------------------------------------------------------------

	/**
	 * Test application entry point; runs all tests.
	 */
	public static void main(String argv[]) {

		// create a clean Minibase instance
		QEPTest qept = new QEPTest();
		qept.create_minibase();

		//creates schema for employees and department
		System.out.println("\nCreating Schema");
		makeSchema();
		
		System.out.println("\nCreating Heapfiles");
		employeeHeapFile = new HeapFile(null);
		departmentHeapFile = new HeapFile(null);

		System.out.println("\nInitializing File Path");		
		//reads in text files and populates heapfile
			String employeeFilePath = ""; 
			String departmentFilePath = "";
			employeeFilePath += argv[0] + "\\Employee.txt";
			departmentFilePath += argv[0] + "\\Department.txt";
			File employeeTextFile = new File(employeeFilePath);
			File departmentTextFile = new File(departmentFilePath);				
			employeeInit(employeeTextFile);
			System.out.println();
			departmentInit(departmentTextFile);
			
		// run all the test cases
		System.out.println("\n" + "Running " + TEST_NAME + "...");
		boolean status = PASS;
		status &= qept.test1();
		status &= qept.test2();
		status &= qept.test3();
		status &= qept.test4();

		// display the final results
		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		} else {
			System.out.println("All " + TEST_NAME
					+ " completed; verify output for correctness.");
		}

	} // public static void main (String argv[])

	private static void makeSchema() {
		// initialize schema for the "Employee" table
		employeeSchema = new Schema(5);
		employeeSchema.initField(0, AttrType.INTEGER, 4, "EmpId");
		employeeSchema.initField(1, AttrType.STRING, 20, "Name");
		employeeSchema.initField(2, AttrType.FLOAT, 20, "Age");
		employeeSchema.initField(3, AttrType.FLOAT, 4, "Salary");
		employeeSchema.initField(4, AttrType.INTEGER, 4, "DeptID");

		// initialize schema for the "Department" table
		departmentSchema = new Schema(4);
		departmentSchema.initField(0, AttrType.INTEGER, 4, "DeptId");
		departmentSchema.initField(1, AttrType.STRING, 30, "Name");
		departmentSchema.initField(2, AttrType.FLOAT, 10, "MinSalary");
		departmentSchema.initField(3, AttrType.FLOAT, 10, "MaxSalary");
	}
	
	
	protected boolean test1() {
		try {

			System.out.println("\nTest 1: SELECT ID, NAME, AGE FROM EMPLOYEE");
			initCounts();

			FileScan scan = new FileScan(employeeSchema, employeeHeapFile);

			// test projection operator
			saveCounts(null);
			System.out.println("\n  ~> test Query 1\n");
			scan = new FileScan(employeeSchema, employeeHeapFile);
			Projection pro = new Projection(scan, 0, 1, 2);
			pro.execute();
			saveCounts("Query1");

			pro = null;
			scan = null;
			System.gc();
			saveCounts("join");

			
			
			// that's all folks!
			System.out.print("\n\nTest 1 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 1 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean test1()
	
	protected boolean test2() {
		try {

			System.out.println("\nTest 2: SELECT NAME FROM DEPARTMENT WHERE MINSALARY = MAXSALARY");
			initCounts();

			// test selection operator
			saveCounts(null);
			Predicate[] preds = new Predicate[] {
					new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 3)};
			
			FileScan scan = new FileScan(departmentSchema, departmentHeapFile);
			Selection sel = new Selection(scan, preds);
			//sel.execute();
			
			
			Projection pro = new Projection(sel, 1);
			pro.execute();
						
			saveCounts("Query 2");


			//pro = null;
			sel = null;
			scan = null;
			System.gc();
			saveCounts("join");

			// that's all folks!
			System.out.print("\n\nTest 2 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 2 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean test2()
	
	protected boolean test3() {
		try {

			System.out.println("\nTest 3: SELECT EMPLOYEE.NAME, DEPARTMENT.NAME, MAXSALARY"
					+ " FROM EMPLOYEE, DEPARTMENT WHERE EMPLOYEE.DEPTID = DEPARTMENT.DEPTID");
			initCounts();

			// test selection operator
			saveCounts(null);
			
			FileScan employeeScan = new FileScan(employeeSchema, employeeHeapFile);
			FileScan departmentScan = new FileScan(departmentSchema, departmentHeapFile);
			
			Predicate[] preds = new Predicate[] {new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 5)};
			SimpleJoin join = new SimpleJoin(employeeScan, departmentScan, preds);
			
			Projection proj = new Projection(join, 1, 6, 8);
			
			proj.execute();
									
			saveCounts("Query 3");


			//pro = null;
			//sel = null;
			//scan = null;
			System.gc();
			saveCounts("join");

			// that's all folks!
			System.out.print("\n\nTest 3 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 3 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean test3()
	
	protected boolean test4() {
		try {

			System.out.println("\nTest 4: SELECT EMPLOYEE.NAME"
					+ " FROM EMPLOYEE, DEPARTMENT "
					+ " WHERE EMPLOYEE.DEPTID = DEPARTMENT.DEPTID & EMPLOYEE.SALARY > DEPARTMENT.MAXSALARY");
			initCounts();

			// test selection operator
			saveCounts(null);
			
			FileScan employeeScan = new FileScan(employeeSchema, employeeHeapFile);
			FileScan departmentScan = new FileScan(departmentSchema, departmentHeapFile);
						
			Predicate[] joinPred = new Predicate[] { new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 5)};		
			Predicate[] selectPred = new Predicate[] { new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FIELDNO, 8)};
			
			SimpleJoin join = new SimpleJoin(employeeScan, departmentScan, joinPred);
			Selection select = new Selection(join, selectPred);
			Projection proj = new Projection(select, 1);
			
			proj.execute();
									
			saveCounts("Query 4");


			//pro = null;
			//sel = null;
			//scan = null;
			System.gc();
			saveCounts("join");

			// that's all folks!
			System.out.print("\n\nTest 4 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 4 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean test4()	
	
	
	protected static void employeeInit(File file){
        try {
            Scanner input = new Scanner(System.in);
            input = new Scanner(file);
            
            //skips first line
            input.nextLine();
            
            while (input.hasNextLine()) {
                String line = input.nextLine();
                List<String> list = new ArrayList<String>(Arrays.asList(line.split(", ")));
    				// create the tuple
                	Tuple tuple = new Tuple(employeeSchema);
                	
    				tuple.setIntFld(0, Integer.parseInt(list.get(0)));
    				tuple.setStringFld(1, list.get(1));
    				tuple.setFloatFld(2, Float.parseFloat(list.get(2)));
    				tuple.setFloatFld(3, Float.parseFloat(list.get(3)));
    				tuple.setIntFld(4, Integer.parseInt(list.get(4)));

    				//tuple.print();
    				
    				// insert the tuple in the file and index
    				RID rid = employeeHeapFile.insertRecord(tuple.getData());

            }
            input.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
	}
	
	protected static void departmentInit(File file){
        try {
            Scanner input = new Scanner(System.in);
            input = new Scanner(file);
            
            //skips first line
            input.nextLine();
            
            while (input.hasNextLine()) {
                String line = input.nextLine();
                List<String> list = new ArrayList<String>(Arrays.asList(line.split(", ")));
    				// create the tuple
                	Tuple tuple = new Tuple(departmentSchema);
                	
    				tuple.setIntFld(0, Integer.parseInt(list.get(0)));
    				tuple.setStringFld(1, list.get(1));
    				tuple.setFloatFld(2, Float.parseFloat(list.get(2)));
    				tuple.setFloatFld(3, Float.parseFloat(list.get(3)));
    				
    				//tuple.print();

    				// insert the tuple in the file and index
    				RID rid = departmentHeapFile.insertRecord(tuple.getData());

            }
            input.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
	}
	
}
