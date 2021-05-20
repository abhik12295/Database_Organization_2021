#------ASSIGNMENT 2-BUFFER MANAGER---- CS-525-----

##	Submittted by Group 10:
	1.KRITI SHREE     - A20467768(kshree@hawk.iit.edu)	
	2.PURVAJ DESAI    - A20469336(pdesai@hawk.iit.edu)
	3.ABHISHEK KUMAR  - A20467982(akumar84@hawk.iit.edu)

***********************************************************************

The assignment Storage Manager comprises of following files:
   1. Five C files:
	1.1. buffer_mgr_stat.c
	1.2. buffer_mgr.c
	1.3. dberror.c
	1.4. storage_mgr.c
	1.5. test_assign2_1.c
   2. Six header files
	2.1.buffer_mgr_stat.h
	2.2.buffer_mgr.h
	2.3.dberror.h
	2.4.dt.h
	2.5.storage_mgr.h
	2.6.test_helper.h
   3. Makefile
   4. README


***********************************************************************


To execute the file, run anyone of the below commands:
1. make
2. makeall

The command generates an output file named "test_assign2_1" and "test_assign2_2". This file can then be executed by using the command: "./test_assign2_1" and "./test_assign2_2"

The output files can be cleaned using the following command:
make clean


***********************************************************************




-----------------------------------------------------------------------------


	Functions created to implement the buffer manager are as below:

	a.) RC initBufferPool
		Initiate the BufferPool with page number using the Page Replacement Strategy

	b.) RC shutdownBufferPool
		Shut down the BUffer Pool
	c.)RC forceFlushPool
		Causes the buffer pool dirty pages (with fix count 0) from to be written to disk
	d.)RC markDirty
		Mark the requested page as Dirty

	e.) RC unpinPage
		Unpins the page

	f.) RC forcePage
		Write the current content of the oage back to the page file on disk
	g.) RC pinPage
		-Pin the page with requested page number in the BufferPool
 		-Load the page from file to BufferPool if page is not there
	h.) RC pinPageLRU:
		-Pin the page with requested page number in the BufferPool
 		-Load the page from file to BufferPool if page is not there
	
	i.) RC pinPageFIFO
		Pin the page with requested page number in the BufferPool using FIFO replacement strategy

	j.) bool *getDirtyFlags
		-Return an array of bools (of size numPages)
 		-Empty page frames are considered as clean
		
	k.) int *getFixCounts
		Return an array of ints (of size numPages) where the fix count of page stored is the nth page frame is the nth element
 
	l.) int getNumReadIO
		Since the Buffer Pool has been initialized, 
 		Return the number of pages written to the page file
	
----------------------------------------------------------------------------------

When an error occurs, the dberror.c file is used to provide an error message.
