#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "test_helper.h"


typedef struct TableData_information{
    int numberOfPages;
    int numberOfRecords;
    int numberOfRecordsPerPage;
    int numberOfTuplesInserted;
    BM_BufferPool *bufferPool;
}TableData_information;

typedef struct RM_SCANDATA_MANAGEMENT{
    RID currentRID; 
    Expr *cond; 
    int totalScan;  
}RM_SCANDATA_MANAGEMENT;

RM_TableData *rm_tabledata = NULL;



/* -----------Table and Record Manager Functions Begins -------------------------*/

/**************************************************************************************
 * Function Name: initRecordManager
 *
 * Description: 
 *		Initializing the record manager
 *		
 * Parameters:  void *mgmtData
 *		
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC initRecordManager (void *mgmtData)
{
    rm_tabledata = (RM_TableData *) malloc (sizeof(RM_TableData));
    (*rm_tabledata).mgmtData = mgmtData;
    return RC_OK;
}


/**************************************************************************************
 * Function Name: shutdownRecordManager
 *
 * Description: 
 *		Shutting down the record manager
 *		
 * Parameters:   NULL
 *		
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC shutdownRecordManager (){
    
    if ((*rm_tabledata).schema != NULL){
        free((*rm_tabledata).schema); 
        (*rm_tabledata).schema = NULL;
    }
    free((*rm_tabledata).mgmtData); 
    (*rm_tabledata).mgmtData = NULL;
    free(rm_tabledata);
    rm_tabledata = NULL;
    
    return RC_OK;
}

/**************************************************************************************
 * Function Name: createTable
 *
 * Description: 
 *		Creating a table
 *		
 *
 * Parameters:
 *		char *name: name of the table. it will create a table with this file name
 *		Schema *schema: contains other information which needed to create a table
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC createTable (char *name, Schema *schema){
    
    if (name == NULL) {
    	return RC_FILE_NOT_FOUND;
    	}
    if (schema == NULL) {
    	return RC_RM_UNKOWN_DATATYPE;
    }
    RC rc;
    (*rm_tabledata).name = name;
    (*rm_tabledata).schema = schema;
    TableData_information *tabledata_info;
    BM_BufferPool *bufferPool;
    
    tabledata_info = ((TableData_information *) malloc (sizeof(TableData_information)));
    (*tabledata_info).numberOfPages = 0;
    (*tabledata_info).numberOfRecords = 0;
    (*tabledata_info).numberOfRecordsPerPage = 0;
    (*tabledata_info).numberOfTuplesInserted = 0;
    
   
    rc = createPageFile((*rm_tabledata).name);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    bufferPool = MAKE_POOL();
    (*bufferPool).mgmtData = malloc(sizeof(buffer));
    rc = openPageFile((*rm_tabledata).name, &bufferPool->fH);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    
       int return_Value;
      	char *first_page = (char *)malloc(PAGE_SIZE);  
        int count = 1;
	char pnt = 'y';
	
	while(count>0){
	switch(pnt)
	{
		case 'n':
			return return_Value = RC_WRITE_FAILED;
		
		case 'y':
		        memcpy(first_page, &tabledata_info->numberOfPages, sizeof(int));
		        first_page += sizeof(int);
		        memcpy(first_page, &tabledata_info->numberOfRecords, sizeof(int));
		        first_page += sizeof(int);
		        memcpy(first_page, &tabledata_info->numberOfRecordsPerPage, sizeof(int));
		        first_page += sizeof(int);
		        memcpy(first_page, &tabledata_info->numberOfTuplesInserted, sizeof(int));
		        first_page -= 3*sizeof(int);
		        strcat(first_page, serializeSchema(schema));				
	}
		count-=1;
	}
    
    if (0 == getBlockPos(&bufferPool->fH))
    {
        rc = writeCurrentBlock(&bufferPool->fH, first_page);
    }
    else
    {
        rc = writeBlock(0, &bufferPool->fH, first_page);
    }
    if (rc != RC_OK)
    {
        return rc;
    }
    
    free(first_page);
    first_page = NULL;
    
    (*tabledata_info).numberOfPages = 0;
    (*tabledata_info).bufferPool = bufferPool;
    (*rm_tabledata).mgmtData = tabledata_info;
    return RC_OK;
    
  }


/**************************************************************************************
 * Function Name: openTable
 *
 * Description: 
 *		Opening a table so that other operations such as scanning and
 *		inserting records can be performed
 *
 * Parameters:
 *		RM_TableData *rel: ata of Relational Table contains 
 *				    all information of schema
 *		char *name: name of the table. it will create a table with this file name
 *		
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/
 
RC openTable (RM_TableData *rel, char *name)
{
    if (name == NULL){
     	return RC_FILE_NOT_FOUND;
	}
    if (rm_tabledata == NULL){ 
        return RC_RM_UNKOWN_DATATYPE;
    }

    RC rc;
    rc = initBufferPool(((TableData_information *)(*rm_tabledata).mgmtData)->bufferPool, name, 10000, RS_CLOCK, NULL);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    *rel = *rm_tabledata;
    return RC_OK;
   
}

/**************************************************************************************
 * Function Name: closeTable
 *
 * Description: 
 *		Closing a table and all outstanding changes to the table
 *		that has to be written to page 0
 *
 * Parameters:
 *		RM_TableData *rel: Data of Relational Table contains 
 *				    all information of schema
 *		
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC closeTable (RM_TableData *rel) 
{
    if (rm_tabledata == NULL) {
    	return RC_RM_UNKOWN_DATATYPE; 
    }
    RC rc;
    rc = shutdownBufferPool(((TableData_information *)(*rm_tabledata).mgmtData)->bufferPool);
    if (rc != RC_OK)
    {
        return rc;
    }
    
    (*rel).name = NULL;  //assignning null to schema name
    return RC_OK;
}

/**************************************************************************************
 * Function Name: deleteTable
 *
 * Description: 
 *		Delete a table and all associated data
 *
 * Parameters:
 *		char *name: name of the table. name of page that has to be deleted
 *		
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC deleteTable (char *name)
{
     if (name == NULL) return RC_FILE_NOT_FOUND; 
     RC rc;
     rc = destroyPageFile(name);
     if (rc != RC_OK)
     {
        return rc;
     }
    
     return RC_OK;
     }


/**************************************************************************************
 * Function Name: getNumTuples
 *
 * Description: 
 *		Returns total number of records in a table
 *
 * Parameters:
 *		RM_TableData *rel: Data of Relational Table contains 
 *				    all information of schema
 *		
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

int getNumTuples (RM_TableData *rel)
{
    return ((TableData_information *)(*rm_tabledata).mgmtData)->numberOfRecords;
}


/* -----------Table and Record Manager Functions Ends -------------------------*/



/* -----------Record Functions Begins -------------------------*/

RC insertrecorddataintofile (Record *record)
{
    if (record == NULL) return RC_RM_UNKOWN_DATATYPE;
    
    RC rc;
    int pageNumber = (*record).id.page;
    int slot = (*record).id.slot;
    BM_BufferPool *bufferPool = ((TableData_information *)rm_tabledata->mgmtData)->bufferPool;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    int offslot_size = getRecordSize(rm_tabledata->schema); 
    
    (*page).data = (char *)malloc(PAGE_SIZE);
    
    rc = pinPage(bufferPool, page, pageNumber);
    if (rc != RC_OK)
        return rc;
    
    (*page).data += slot * (offslot_size);
    memcpy((*page).data, (*record).data, offslot_size);
    (*page).data -= slot * (offslot_size);
        
    rc = markDirty(bufferPool, page);
    if (rc != RC_OK)
        return rc;
   
    rc = unpinPage(bufferPool, page);
    if (rc != RC_OK)
        return rc;

    rc = forcePage(bufferPool, page);
    if (rc != RC_OK)
        return rc;
    
    return RC_OK;
}

/**************************************************************************************
 * Function Name: insertRecord
 *
 * Description: 
 *		The record passed in the input parameter is inserted 
 *		into the available page and slot
 *		
 *
 * Parameters:
 *		RM_TableData *rel: Data of Relational Table contains 
 *				    all information of schema
 *		Record *record: collection of fields or datatypes and new record will 
 *				get updated into file
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC insertRecord (RM_TableData *rel, Record *record)
{
        if (rel == NULL)
    {
        return -1;
    }
    
    if (record == NULL)
    {
        return -1;
    }
    RC rc;
    int offslot_size = getRecordSize(rm_tabledata->schema);
    TableData_information *tabledata_info = (TableData_information *)rm_tabledata->mgmtData;
  
    (*tabledata_info).numberOfRecordsPerPage = PAGE_SIZE/offslot_size;
    (*tabledata_info).numberOfRecords += 1;
    (*tabledata_info).numberOfTuplesInserted += 1;
    
    int available_slot = ((*tabledata_info).numberOfTuplesInserted)%((*tabledata_info).numberOfRecordsPerPage); 
 
    if (available_slot == 1) (*tabledata_info).numberOfPages += 1;
    (*record).id.page = (*tabledata_info).numberOfPages; 
    if (available_slot == 0) (*record).id.slot = (*tabledata_info).numberOfRecordsPerPage - 1;
    else (*record).id.slot = available_slot - 1; 
    
    rc = insertrecorddataintofile(record);
    if (rc != RC_OK)
    {
        return rc;
    }
    (*rm_tabledata).mgmtData = tabledata_info;
    *rel = *rm_tabledata;
    return RC_OK;
}

/**************************************************************************************
 * Function Name: deleteRecord
 *
 * Description: 
 *		Delete the record of passed input parameter
 *		
 *
 * Parameters:
 *		RM_TableData *rel:  Data of Relational Table contains 
 *				     all information of schema
 *		RID id: 	     it contains page number and slot numnber
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC deleteRecord (RM_TableData *rel, RID id) 
{
    
     if (rel == NULL)
        return -1;

    int pageNumber = id.page; 
    int slot = id.slot; 
    
    RC rc;
    int offslot_size = getRecordSize((*rm_tabledata).schema);
    TableData_information *tabledata_info = ((TableData_information *)(*rm_tabledata).mgmtData);
    BM_BufferPool *bufferPool = (*tabledata_info).bufferPool;
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    (*page).data = (char *)malloc(PAGE_SIZE);
    
    rc = pinPage(bufferPool, page, pageNumber);
    if (rc != RC_OK)
        return rc;
    
    (*page).data += (offslot_size)*slot;
    memset((*page).data, 0, offslot_size);
    (*page).data -= (offslot_size)*slot;
 
    rc = markDirty(bufferPool, page);
    if (rc != RC_OK)
        return rc;

    rc = unpinPage(bufferPool, page);
    if (rc != RC_OK)
        return rc;

    rc = forcePage(bufferPool, page);
    if (rc != RC_OK)
        return rc;
        
    (*tabledata_info).numberOfRecords -= 1;
    
    (*rm_tabledata).mgmtData = tabledata_info;
    *rel = *rm_tabledata;
    return RC_OK;
}


/**************************************************************************************
 * Function Name: updateRecord
 *
 * Description: 
 *		Particular record at page and slot mentioned in record
 *		will be updated
 *
 * Parameters:
 *		RM_TableData *rel: Data of Relational Table contains 
 *				    all information of schema
 *		Record *record:    collection of fields or datatypes and new record will 
 *				    get updated into file
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC updateRecord (RM_TableData *rel, Record *record)
{
    RC rc;
    if (rel == NULL)
        return -1;

    if (record == NULL)
        return -1;
    rc = insertrecorddataintofile(record);

    if (rc != RC_OK)
        return rc;
    
    *rel = *rm_tabledata;
    return RC_OK;
}

/**************************************************************************************
 * Function Name: getRecord
 *
 * Description: 
 *		Return the record of specific page number and slot number
 *		passed in input parameter
 *
 * Parameters:
 *		RM_TableData *rel: Data of Relational Table contains 
 *				    all information of schema
 *		Record *record:    collection of fields or datatypes and new record will 
 *				    get updated into file
 *		RID id: 	    it contains page number and slot numnber
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    RC rc;
    if ((rm_tabledata == NULL)||(record == NULL))
        return RC_RM_UNKOWN_DATATYPE;
    int pageNumber = id.page; 
    int slot = id.slot; 
    (*record).id = id; 
    int offslot_size = getRecordSize((*rm_tabledata).schema); 
    TableData_information *temp = (TableData_information *)(*rm_tabledata).mgmtData;
    
    BM_BufferPool *bufferPool = (*temp).bufferPool;
    BM_PageHandle *page = MAKE_PAGE_HANDLE();
    
    rc = pinPage(bufferPool, page, pageNumber);
    if (rc != RC_OK)
        return rc;
    
    (*page).data += (offslot_size)*slot;
    memcpy((*record).data, (*page).data, offslot_size); 
    (*page).data -= (offslot_size)*slot;

    rc = unpinPage(bufferPool, page);
    if (rc != RC_OK)
        return rc;
    
    return RC_OK;
    
}

/* -----------Record Functions Ends -------------------------*/


/* -----------Scan Functions Begins -------------------------*/

/**************************************************************************************
 * Function Name: startScan
 *
 * Description: 
 *		Retrieve all tuples from a table that fulfill a certain condition
 *
 * Parameters:
 *		RM_TableData *rel   : Data of Relational Table contains 
 *				       all information of schema
 *		RM_ScanHandle *scan : Initiates to start handling and perform scan about
 *					all informations
 *		Expr *cond          : Condition to scan data
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{

    if ((rel == NULL)||(scan == NULL)||(cond == NULL)){
     	return RC_RM_UNKOWN_DATATYPE;
     }
    RM_SCANDATA_MANAGEMENT *scan_management = ((RM_SCANDATA_MANAGEMENT *) malloc (sizeof(RM_SCANDATA_MANAGEMENT)));
    
    int return_Value;
    int counter = 1;
    char point = 'z';
	
    while(counter>0){
    switch(point)
	{
		case 'x':
			return_Value = RC_FILE_NOT_FOUND;
		
		case 'z':
			(*scan_management).totalScan = 0;
		        (*scan_management).cond = cond;
		        (*scan_management).currentRID.page = 1;
		        (*scan_management).currentRID.slot = 0;
		        (*scan).mgmtData = scan_management;
		        (*scan).rel = rel;
		        return RC_OK;
	}
	counter-=1;					
	}
    
    }
   



/**************************************************************************************
 * Function Name: next
 *
 * Description: 
 *		Return the next tuple that fulfills the scan the condition
 *
 * Parameters:
 *		RM_ScanHandle *scan : Initiates to start handling and perform scan about
 *					all informations
 *		Record *record      : all the data from rows or collection of fields
 *
 * Return:
 *		RC: Return Code
 *
 **************************************************************************************/

RC next (RM_ScanHandle *scan, Record *record)
{
   
    if ((scan == NULL)||(record == NULL)) {
    	return RC_RM_UNKOWN_DATATYPE;
    }
    RC rc;
    RM_SCANDATA_MANAGEMENT *scan_management = (RM_SCANDATA_MANAGEMENT *)(*scan).mgmtData;
 
    TableData_information *tabledata_info = (TableData_information *)(*rm_tabledata).mgmtData;
    
    Value *result = ((Value *) malloc (sizeof(Value)));
    
    (*result).v.boolV = FALSE;
    int time_record = 10;
    int scanned = 2;
    for (int counter = 0; counter<1; counter++){
		switch(scanned){
			case 1:
				while (time_record>5){
					printf("Can't write the block! ");
				}
			
			case 2:	
				while(!(*result).v.boolV)
    {
        
        if ((*scan_management).totalScan == (*tabledata_info).numberOfRecords)
         {
         	return RC_RM_NO_MORE_TUPLES;
        }
        rc = getRecord (rm_tabledata, (*scan_management).currentRID, record); 
        if (rc != RC_OK)
            return rc;
        
        rc = evalExpr (record, (*rm_tabledata).schema, (*scan_management).cond, &result);
        (*scan_management).currentRID.slot ++; 
        if ((*scan_management).currentRID.slot == (*tabledata_info).numberOfRecordsPerPage)
        {
            (*scan_management).currentRID.page += 1;
            (*scan_management).currentRID.slot = 0;
        }
        (*scan_management).totalScan ++; 
        
    }
    (*scan).mgmtData = scan_management;
    return RC_OK;		
						
   }
    }
}


RC closeScan (RM_ScanHandle *scan)
{
     if (scan == NULL) {
     	return RC_RM_UNKOWN_DATATYPE;
  	 }	
    free((*scan).mgmtData);
    (*scan).mgmtData = NULL;
    return RC_OK;
}

/* -----------Scan Functions Ends -------------------------*/



/* -----------Schema Functions Begins -------------------------*/


int getRecordSize (Schema *schema)
{
    
    if (schema == NULL)
    {
        return RC_RM_SCHEMA_NOT_FOUND;
    }
    int sizeOfRecord = 0;
    
    int i =0;
    while(i < (*schema).numAttr)
    {
  
        switch((*schema).dataTypes[i])
        {
            case DT_FLOAT:
                sizeOfRecord += sizeof(float);
                break;
            case DT_BOOL:
                sizeOfRecord += sizeof(bool);
                break;
            case DT_INT:
                sizeOfRecord += sizeof(int);
                break;
            case DT_STRING:
                sizeOfRecord += (*schema).typeLength[i]+1;
                break;
            default:
                return RC_RM_UNKOWN_DATATYPE;
        }
    i++;
    }
    return sizeOfRecord; 
    }
    

static Schema *mallocSchema(int numAttr, int keySize)
{
    Schema *SCHEMA;
    
   
    SCHEMA = (Schema *)malloc(sizeof(Schema));
    (*SCHEMA).numAttr = numAttr;
    (*SCHEMA).attrNames = (char **)malloc(sizeof(char*) * numAttr);
    (*SCHEMA).typeLength = (int *)malloc(sizeof(int) * numAttr);
    (*SCHEMA).dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
    (*SCHEMA).keyAttrs = (int *)malloc(sizeof(int) * keySize);
    (*SCHEMA).keySize = keySize;
    
    int i =0;
    while( i < numAttr)
    {
        (*SCHEMA).attrNames[i] = (char *) malloc(sizeof(char *));
        i++;
    }
    
    return SCHEMA;
}    


/**************************************************************************************
 * Function Name: createSchema
 *
 * Description: 
 *		create schema such as attribute, number, name, data, type and other 
 *		parameters
 *
 *					
 * Return:
 *		return Schema
 *
 **************************************************************************************/
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *schema = mallocSchema(numAttr, keySize);
   
    (*schema).numAttr = numAttr;
    (*schema).dataTypes = dataTypes;
    (*schema).typeLength = typeLength;
    (*schema).keySize = keySize;
    (*schema).keyAttrs = keys;
    int i =0;
    while (i< numAttr)
    {
        strcpy((*schema).attrNames[i], attrNames[i]);
        i++;
    }
    return schema;
}

/**************************************************************************************
 * Function Name: freeSchema
 *
 * Description: 
 *		Free the memory which was allocated to schema
 *
 * Parameters:
 *		Schema *schema : 
 *					
 * Return:
 *		RC_OK: Return Code
 *
 **************************************************************************************/

RC freeSchema (Schema *schema){
    if (schema == NULL)
    {
        return RC_RM_SCHEMA_NOT_FOUND;
    }
  
    free((*schema).attrNames);
    (*schema).attrNames = NULL;
    free(schema->dataTypes);
    (*schema).dataTypes = NULL;
    free((*schema).typeLength);
    (*schema).typeLength = NULL;
    free((*schema).keyAttrs);
    (*schema).keyAttrs = NULL;
    free(schema);
    schema = NULL;
    
    return RC_OK;
}

/* -----------schema functions begins -------------------------*/

/* -----------Attribute functions begins -------------------------*/

/**************************************************************************************
 * Function Name: createRecord
 *
 * Description: 
 *		new record with all the null values will be created
 *
 * Parameters:
 *		Record *record : 
 *		Schema *schema : 
 *					
 * Return:
 *		RC_OK: Return Code
 *
 **************************************************************************************/


RC createRecord (Record **record, Schema *schema) 
{
  if (schema == NULL) 
  {
  	return RC_RM_SCHEMA_NOT_FOUND;
   }
   *record = ((Record *) malloc (sizeof(Record)));
   (*(*record)).data = (char *)malloc(getRecordSize(schema));
    
   return RC_OK;

}


/**************************************************************************************
 * Function Name: freeRecord
 *
 * Description: 
 *		free the memory related to record
 *
 * Parameters:
 *		Record *record : 
 *					
 * Return:
 *		RC_OK: Return Code
 *
 **************************************************************************************/

RC freeRecord (Record *record)
{
     if (record == NULL){
     	 return -1;
    }
    free((*record).data);
    (*record).data = NULL;
    free(record);
    record = NULL;
    
    return RC_OK;
}

/**************************************************************************************
 * Function Name: getAttr
 *
 * Description: 
 *		the value of attribute pointed by attributes will be return 
 *
 * Parameters:
 *		Record *record : records data
 *		Schema *schema : schema attributes 
 *		int attrNum	: attribute number
 *		Value **value	: 	
 *		
 *					
 * Return:
 *		RC_OK: Return Code
 *
 **************************************************************************************/

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    if (record == NULL) {
    	return -1;
    	}
    if (schema == NULL) {
    	return -1;
    	}
    RC rc;
    char *record_data = (*record).data;
    int offsetattribute = 0;
    value[0] = ((Value *) malloc (sizeof(Value)));
    
    rc = attrOffset(schema, attrNum, &offsetattribute);
    if (rc != RC_OK){
        return rc;
    }
    
    record_data += offsetattribute;

    switch((*schema).dataTypes[attrNum])
    {
        case DT_FLOAT:
            memcpy(&((*value[0]).v.floatV), record_data, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(&((*value[0]).v.boolV), record_data, sizeof(bool));
            break;
        case DT_INT:
            memcpy(&((*value[0]).v.intV), record_data, sizeof(int));
            break;
        case DT_STRING:
            (*value[0]).v.stringV = (char *)malloc((*schema).typeLength[attrNum] + 1);
            memcpy(((*value[0]).v.stringV), record_data, (*schema).typeLength[attrNum] + 1);
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
            
    }
    record_data -= offsetattribute;
    (*value[0]).dt = (*schema).dataTypes[attrNum];
    return RC_OK;
}


/**************************************************************************************
 * Function Name: setAttr
 *
 * Description: 
 *		set the value of particular attribute given in attribute number
 *
 * Parameters:
 *		Record *record : records data
 *		Schema *schema : schema attributes 
 *		int attrNum	: attribute number
 *		Value **value	: 	
 *				
 * Return:
 *		RC_OK: Return Code
 *
 **************************************************************************************/

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    
    RC rc;
    if (record == NULL) {
    	return -1;
    	}
    if (schema == NULL) {
    	return -1;
    	}
    if (attrNum < 0 || attrNum >= (*schema).numAttr) 
    {	
    	return -1;
    }
    char *record_data = (*record).data;
    int offsetattribute = 0;
    rc = attrOffset(schema, attrNum, &offsetattribute);
    record_data += offsetattribute;
    (*schema).dataTypes[attrNum] = (*(value)).dt;
    switch((*value).dt){
        case DT_INT:
            memcpy(record_data, &((*value).v.intV), sizeof(int));
            break;
        case DT_STRING:
            memcpy(record_data, ((*value).v.stringV), (*schema).typeLength[attrNum] + 1);
            break;
        case DT_FLOAT:
            memcpy(record_data, &((*value).v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(record_data, &((*value).v.boolV), sizeof(bool));
            break;
        default:
            return RC_RM_UNKOWN_DATATYPE;
    }
    record_data -= offsetattribute;
    
    return RC_OK;
}



