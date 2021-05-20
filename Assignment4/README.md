##------ASSIGNMENT 4 - B+ Tree---- CS-525-----

##Submittted by Group 10:
	1.KRITI SHREE     - A20467768(kshree@hawk.iit.edu)  
	2.PURVAJ DESAI    - A20469336(pdesai@hawk.iit.edu)
	3.ABHISHEK KUMAR  - A20467982(akumar84@hawk.iit.edu)

***********************************************************************


To execute the file, run anyone of the below commands:
1. make
2. ./test_assign4_1
3. ./test_assign4_2
4. ./test_assign4_3

***********************************************************************


DESCRIPTION OF FUNCTIONS:
-----------------------------------------------------------------------------


	1) initIndexManager (void *mgmtData)

	Set up the index manager.


2) shutdownIndexManager ()

	Close index manager, as well as unrestricted resources.

3) createBtree (char *idxId, DataType keyType, int n)

	Create a B+ tree and apply resources.


4) openBtree (BTreeHandle **tree, char *idxId)

	Open a B+ tree that already exists.


5) closeBtree (BTreeHandle *tree)

	Free resources by closing a B+ tree.

6) deleteBtree (char *idxId)

	Delete a B+ tree from disk.


7) getNumNodes (BTreeHandle *tree, int *result)

	Calculate the number of nodes in a B+ tree.


8) getNumEntries (BTreeHandle *tree, int *result)

	Calculate the number of entries in a B+ tree.

9) getKeyType (BTreeHandle *tree, DataType *result)

	Get the B+ tree's Key Form.


10) findKey (BTreeHandle *tree, Value *key, RID *result)

	Return the RID result after finding a key in the B+ tree.


11) insertKey (BTreeHandle *tree, Value *key, RID rid)

	Insert a key into a B+ tree.


12) deleteKey (BTreeHandle *tree, Value *key)

	Delete a key from a B+ tree.


13) openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)

	Set up the scan handle and get ready to scan.

14) nextEntry (BT_ScanHandle *handle, RID *result)
	
	During scanning, return to the next entry.


15) closeTreeScan (BT_ScanHandle *handle)

	Stop scanning and free resources.


16) printTree (BTreeHandle *tree)

	Return the tree's printing in the format This is a debugging feature.


17) DFS(RM_BtreeNode *bTreeNode)

	Perform a pre-order tree walk and assign tree node positions.


18) walk(RM_BtreeNode *bTreeNode, char *result)

	Generate the printing string by doing a pre-order tree walk.


19) createNewNod()

	Return a B+ tree node that has just been created.


20) insertParent(RM_BtreeNode *left, RM_BtreeNode *right, int key)

	Recursively insert the key into non-leaf B+ tree nodes.


21) deleteNode(RM_BtreeNode *bTreeNode, int index)

	If necessary, delete the node in a recursive manner.


ADDITIONAL IMPLEMENTATION and TEST CASES (Conditional Updates using Scans):
------------------------------------------------------------------------------------------



The test cases are as follows:
1. testInsertAndFind_String
	a. Verifies that the values have been correctly entered into the B tree
	b. Verifies the number of enteries and compares it to the RID.

2. testDelete_Float
	a. searches for the value to delete;
	b. uses the RID to search for the value; 
	c. after the value is found, it is deleted;
 	d. after the delete, a search procedure is performed once more.
	e. This is to prevent the deleted object from being discovered again.
3. testInsertAndFind_Float
	
	a. Checks for the number of nodes and enteries in the B tree. 
	b. Iterates through the loop(500).
	c.In step 2, we essentially do a search to find the key.

4. testDelete_String
	a. compare the RID to the entry (among the inserted enteries as well)
	b. The RID for the deleted entery should not be identified.
	c. For the ones that aren't deleted, when we find the correct entry, we compare it to the RID - to ensure that it's correct.

5. createPermutation
	a. This is used to generate various value permutations.
	b. It is necessary to use a temporary variable, an iterator, and a variable named res.
	c. The res value is returned

6. createValues
	a. New values are created.
	b. A conversion from a string to a value is performed.
	c. The outcome is saved in res.

7. freeValues
	a. The value is simply freed with the free function.
	
------------------------------------------------------------------------------------------------
