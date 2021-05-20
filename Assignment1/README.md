#------ASSIGNMENT 1-STORAGE MANAGER---- CS-525-----

##	Submittted by Group 10:
	1.KRITI SHREE     - A20467768(kshree@hawk.iit.edu)	
	2.PURVAJ DESAI    - A20469336(pdesai@hawk.iit.edu)
	3.ABHISHEK KUMAR  - A20467982(akumar84@hawk.iit.edu)

***********************************************************************

##The assignment Storage Manager comprises of following files:
##   1. Three C files:
##	1.1. storage_mgr.c
##	1.2. storage_mgr.h
##	1.3. test_assign1.c
##  2. Three header files
##	2.1.test_helper.h
##	2.2.dberror.c
##	2.3.dberror.h
##   3. Makefile
##   4. README

***********************************************************************


To execute the file, run anyone of the below commands:
1. make
2. makeall

The command generates an output file named "test_assign1_1". This file can then be executed by using the command: "./test_assign1_1"

The output files can be cleaned using the following command:
make clean


***********************************************************************

TEST CASES:

New test case has been added in the file test_assign1_1.c namely:

1. new_page_test(): 
	-Tests the content of a single page


***********************************************************************


storage_mgr.c has all the definitions of the interfaces declared in storage_mgr.h. The implementation of functions used in storage_mgr.c has been described below:

readBlock()
readFirstBlock() 
readCurrentBlock()
readPreviousBlock()
readNextBlock()
readLastBlock()
writeBlock ()
writeCurrentBlock()
appendEmptyBlock()
ensureCapacity ()

Current Page Position=0 when the file is opened

DESCRIPTION OF FUNCTIONS:
-----------------------------------------------------------------------------


	1.Functions which manipulate page file(CREATE, OPEN, CLOSE, DESTROY)

	a.) createPageFile():
		-Creates a new file of page size 1 and fills it with '\0' bytes.
		-Opens the file in write mode and fills it.

	b.) openPageFile():
		-Checks if the input file is present
		-If file is present then opens the file in read and write mode.
		-Calculates the total number of pages.
		-Stores the total Number of Pages, current Page Position and fileName
	c.)closePageFile()
		-Closes the file using fclose()
		-If file is closed successfully then it returns RC_OK else it returns RC_FILE_NOT_FOUND
	d.)destroyPageFile():
		-Deletes the file using "remove" function 
		-If file is destroyed successfully then it returns RC_OK else it returns RC_FILE_NOT_FOUND


-------------------------------------------------------------------------------

	2. Functions for reading blocks from disc:

	a.) readBlock():
		-Checks if the page exists using pageNum.
		-If the page exists then it sets the file pointer using fseek 
		-Read the file using fread and then set the current page position at the page that has been read.

	b.) getBlockPos():
		-Returns the current page position by using fhandle.

	c.) readFirstBlock():
		-If fhandle is not equal to Null and total number of pages are greater than 0, then it reads the first block of the file.
	
	d.) readPreviousBlock():
		-If the datastructure fhandle is not equal to Null, then it sets the pointer to the previous page position else throws error.
		-If the previous block is not within the page limit then it throws an error.
		-If the previous block is within the page limit then it reads the block and updates the current page position reducing it by 1.
	
	e.) readCurrentBlock():
		- If the datastructure fhandle is not equal to Null, then it gets the positions of the current page by calling getBlockPos().
		- Reads the content of the current block using fhandle.

	f.) readNextBlock():
		- If the datastructure fhandle is not equal to Null, then it sets the position of pointer to the next block position by increasing the current block position by 1.
		- Reads the content of the next block of the file.
		
	g.) readLastBlock():
		-If the datastructure fhandle is not equal to Null, then it sets the position of the pointer to the last page.
		- Reads the content of the last block using fread.
		

-------------------------------------------------------------------------------

	3. Functions for writing blocks to a page file:

	a.) writeBlock():
		-Checks if pageNum is greater than totalNumber of pages or less than zero. If yes then it throws error else checks whether fhandle is Null or not.
		-If it is not Null then we set the poitner to the position where we have to write and then write function is executed in the block.
		-Updates the value of totalNumPages which increase by 1.

	b.) writeCurrentBlock():
		-Firstly, the position of the current block is fetched  by calling getBlockPos function 
		-then writeBlock function is called to write block at current position.
		-If current block is written successfully then RC_OK is returned else RC_WRITE_FAILED is returned.

	c.) appendEmptyBlock():
		-Checks if the file is present or not and can be opened in read mode. If all of these conditions are met then the file pointer is set to the end of the file using fseek.
		-Write an empty block then update the file handle variable.

	d.)ensureCapacity():
		-Checks if new capacity is greater than current capacity. 
		-If yes then add number of pages by calling the appendEmptyBlock function.

----------------------------------------------------------------------------------

When an error occurs, the dberror.c file is used to provide an error message.
