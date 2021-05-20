##------ASSIGNMENT 3-RECORD MANAGER---- CS-525-----

##Submittted by Group 10:
	1.KRITI SHREE     - A20467768(kshree@hawk.iit.edu)  
	2.PURVAJ DESAI    - A20469336(pdesai@hawk.iit.edu)
	3.ABHISHEK KUMAR  - A20467982(akumar84@hawk.iit.edu)

***********************************************************************


To execute the file, run anyone of the below commands:
1. Execute the make file 1 as below: 
		make -f makefile1

2. Execute the make file 2 as below: 
		make -f makefile2

3. Execute the make file 3 as below: 
		make -f makefile3

***********************************************************************


DESCRIPTION OF FUNCTIONS:
-----------------------------------------------------------------------------


	1.Table and manager:

	a.) initRecordManager:
		-It's used to set up the record manager.

	b.) shutdownRecordManager:
		-Record Manager was shut down with this command.
		
	c.) createTable:
		- It's used to make a new table. 
		-The name of the table, as well as the schema and free space details, are all included.
	d.)openTable:
		- Used to display information from a table. 
		- Creates a buffer pool before pinning a page. 
		- Reads the file's schema - Unpins the tab
	e) closeTable:
		- To close a table, use this. 
		- Pin the page and mark it as dirty. 
		- Unpin the tab, then close the buffer pool.

	f) deleteTable:
		-The destroyPageFIle parameter is used to remove the table. If it is not RC OK, the page will not be removed.

	g) getNumTuples:
		- Gets the total number of tuples in a table.



-------------------------------------------------------------------------------

	2. Handling records in a table:

	a.) insertRecord:
		- It's used to add a new record to the specified page and slot. 
		- The record should be placed in the empty or usable room. 
		- The record is inserted after the page is pinned. 
		- The page is marked as dirty and written back to memory after the record is inserted. 
		- The page is later unpinned.
	b.) deleteRecord:
		- It's used to remove a record from the specified page and slot. 
		- The record is removed after the page is pinned. 
		- The number of records in a page is reduced by one when a record is removed. 
		- The page is marked as dirty after the record is removed. 
		- The page is later unpinned.
	c.) updateRecord:
		- It's used to update an existing record on the specified page and slot. 
		- The page is pinned, and the record is then updated. 
		- The page is marked as dirty after the record is changed. 
		- The page is later unpinned.
	
	d.) getRecord:
		- To close a table, use this. 
		- Pin the page and mark it as dirty. 
		- Unpin the tab, then close the buffer pool.
		

-------------------------------------------------------------------------------

	3. Scans:

	a) startScan:
		- It is used to search tuples according to a set of parameters (expr). 
		- The RM ScanHandle data structure is initialized when you start scanning.
	b) next:
		- The next method is called by the start scan function. 
		- It returns the rcord's next tuple that meets the requirements specified in start scan. 
		- If the scan criterion is NULL, all tuples are scanned and the result is returned. RC RM NO MORE TUPLES 
	c) closeScan:
		- It's used to let the record manager know that all related resources are ready to be cleaned up.

----------------------------------------------------------------------------------

	4. Dealing with schemas:

	a) getRecordSize: 
		- Checks if the schema has been created; 
		- If schema exists, it returns the size of a schema's records in bytes.
	b) createSchema:
		-To build a new schema, use this method.

	c) freeSchema:
		- This function is used to free memory space associated with the specific schema.

----------------------------------------------------------------------------------

	5. Dealing with records and attribute values:

	a) createRecord:
		- These functions are used to generate a new record. 
		- The page and slot are initially set to -1 because the record has not yet been loaded into the table/page/slot.

	b) freeRecord:
		- Checks to see if the record is available. 
		- If the record is free, it restores record free. 
		- If the record is not free, it frees it by deleting the data from it.

	c) getAttr:
		- It's used to find out what an attribute's value is for a specific record.
		- The requested value might be an Integer, String, or Float.
	d) setAttr:
		- It's used to change the meaning of an attribute. 
		- The value of a specific attribute in a record is set to the given value, which can be of the form Integer, String, or Float.

----------------------------------------------------------------------------------


