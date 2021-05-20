#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

SM_FileHandle filehandler;
SM_PageHandle pagehandler;

TableData_information info_table;
RM_SCANDATA_MANAGEMENT scan_data;



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
    initStorageManager(); 
    printf("\n-------------------------------Initialized a Record Manager -------------------------------");
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
    
    (pagehandler == ((char *)0)) ? printf(" \n ----------------------------Shutdown Record Manager success-----------------------") ,RC_OK : (free(pagehandler)) , RC_OK; 
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

	if(createPageFile(name) == RC_OK)
    {
        int recordsize;
        pagehandler = (SM_PageHandle) malloc(PAGE_SIZE);

        char metadata_Table[PAGE_SIZE];
        memset(metadata_Table,'\0',PAGE_SIZE);

        sprintf(metadata_Table,"%s|",name);

        recordsize = getRecordSize(schema);
        sprintf(metadata_Table+ strlen(metadata_Table),"%d[",(*schema).numAttr);
        int j = 0;
        do
        {
            sprintf(metadata_Table+ strlen(metadata_Table),"(%s:%d~%d)",(*schema).attrNames[j],(*schema).dataTypes[j],(*schema).typeLength[j]);
            j++;
        }while(j < (*schema).numAttr);

        sprintf(metadata_Table+ strlen(metadata_Table),"]%d{",(*schema).keySize);
        j = 0;
        while(j < (*schema).keySize)
        {
            sprintf(metadata_Table+ strlen(metadata_Table),"%d",(*schema).keyAttrs[j]);
            if(j > (*schema).keySize-1)
            {
                return 1;
            }
            else
            {
                strcat(metadata_Table,":");
            }
            j++;
        }
        
        int return_Value;
        
        int count = 1;
	char pnt = 'y';
	
	while(count>0){
	switch(pnt)
	{
		case 'n':
			return return_Value = RC_WRITE_FAILED;
		
		case 'y':
			
			strcat(metadata_Table,"}");
			int x=1, y=0;
			for (int opt = 0; opt<1; opt++)
			{
				info_table.free_space_lctn.page =x;
				for (int val_o = 0; val_o<1; val_o++){
				info_table.free_space_lctn.slot =y;
				info_table.totalRecords =y;}
			}
			
			for (int val = 0; val<1; val++)
			{
			char ch = 's';
			
			switch(ch)
			{
				case 's':
						sprintf(metadata_Table + strlen(metadata_Table),"$%d:%d$",
						info_table.free_space_lctn.page,
						info_table.free_space_lctn.slot);
						char find = 'r';
						switch(find)
						{
							case 'r':
								sprintf(metadata_Table + strlen(metadata_Table),"?%d?",
						info_table.totalRecords);
						
						}
						
			}
			memmove(pagehandler,metadata_Table,PAGE_SIZE);
			}	
					
	}
		count-=1;
	}
	
        
        
        if((openPageFile(name, &filehandler) != RC_OK))
        {
            return 1;
        }
        else if((writeBlock(0, &filehandler, pagehandler)!= RC_OK))
        {
            return 1;
        }
        else
        {
            return 0;
        }
        free(pagehandler);
        return RC_OK;
    }
    
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
    pagehandler = (SM_PageHandle) malloc(PAGE_SIZE);

    BM_PageHandle *h;
    h = &info_table.pageHandle;

    BM_BufferPool *bm;
    bm = &info_table.bufferPool;

    initBufferPool(bm, name, 3, RS_FIFO, NULL);
    bool pin = (pinPage(bm, h, 0));
    if(pin == RC_OK)
    {
        read_Schema(rel,h);
        (unpinPage(bm,h) == RC_OK) ? RC_OK : (RC_message = "Unpin Page Failed"), RC_UNPIN_PAGE_FAILED;
    }
    if (pin != RC_OK)
    {
        RC_message = "Pin page failed ";
        return RC_PIN_PAGE_FAILED;
    }
   
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
    char metadata_Table[PAGE_SIZE];
    memset(metadata_Table,'\0',PAGE_SIZE); 

    sprintf(metadata_Table,"%s|",(*rel).name);
    int recordsize;
    recordsize = info_table.recordsize; 

    sprintf(metadata_Table+ strlen(metadata_Table),"%d[",(*rel).schema->numAttr);
    int j = 0; 
    int a = (*rel).schema->numAttr;
    while ( j < a)
    {
        sprintf(metadata_Table+ strlen(metadata_Table),"(%s:%d~%d)",(*rel).schema->attrNames[j],(*rel).schema->dataTypes[j],(*rel).schema->typeLength[j]);
        j++;
    }
    
    sprintf(metadata_Table+ strlen(metadata_Table),"]%d{",(*rel).schema->keySize);
    int i = 0;
    do 
    {
        sprintf(metadata_Table+ strlen(metadata_Table),"%d",(*rel).schema->keyAttrs[i]);
        if(i>((*rel).schema->keySize-1))
        {
            return 1;
        }
        else
        {
            strcat(metadata_Table,":");
        }
        i++;   
    } while(i < (*rel).schema->keySize);
    
    strcat(metadata_Table,"}"); 

    sprintf(metadata_Table+ strlen(metadata_Table),"$%d:%d$",info_table.free_space_lctn.page,info_table.free_space_lctn.slot);

    sprintf(metadata_Table+ strlen(metadata_Table),"?%d?",info_table.totalRecords);

    BM_PageHandle *page = &info_table.pageHandle;
    BM_BufferPool *bm = &info_table.bufferPool;

     while(pinPage(bm,page,0) != RC_OK){
        RC_message = "Pin page failed ";
        return RC_PIN_PAGE_FAILED;
    }

    memmove((*page).data,metadata_Table,PAGE_SIZE);

    while(markDirty(bm,page)!=RC_OK){
        RC_message = "Page 0 Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }

    while( unpinPage(bm,page)!=RC_OK){
        RC_message = "Unpin Page 0 failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }


    while(shutdownBufferPool(bm) != RC_OK){
        RC_message = "Shutdown Buffer Pool Failed";
        return RC_BUFFER_SHUTDOWN_FAILED;
    }

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
     if(name != ((char *)0))
    {
        (destroyPageFile(name) == RC_OK) ? RC_OK : (RC_message = "Destroy Page File Failed"), RC_FILE_DESTROY_FAILED;
    }
    else
    {
        RC_message = "Table name can not be null ";
        return RC_NULL_IP_PARAM;
    }

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


//returns the total numbers of records in table
int getNumTuples (RM_TableData *rel)
{
    int returnCode = info_table.totalRecords;
    return returnCode;
}


/* -----------Table and Record Manager Functions Ends -------------------------*/



/* -----------Record Functions Begins -------------------------*/

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
    BM_PageHandle *page;
    page = &info_table.pageHandle;
    BM_BufferPool *bm;
    bm = &info_table.bufferPool;
    char *pageData;   						      
    int recordsize;
    recordsize = info_table.recordsize;
    int free_Page;
    free_Page = info_table.free_space_lctn.page;  
    int free_Slot;
    free_Slot = info_table.free_space_lctn.slot;  
    
    
    if(pinPage(bm,page,free_Page) != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }

    while(free_Page < 1)
    {
        RC_message = "Invalid page|Slot number ";
        return RC_IVALID_PAGE_SLOT_NUM;
    }
    while(free_Slot < 0)
    {
        RC_message = "Invalid page|Slot number ";
        return RC_IVALID_PAGE_SLOT_NUM;
    }
    
    pageData = (*page).data; 

    int offset =  free_Slot * recordsize; 
    int blockfactor = info_table.blockFactor;
    
    (*record).data[recordsize-1]='$';
    memcpy(pageData+offset, (*record).data, recordsize); 
    RC mark = markDirty(bm,page);
    RC unpin = unpinPage(bm,page);
    if( mark!=RC_OK){
        RC_message = "Page Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }

    if(unpin!=RC_OK){
        RC_message = "Unpin Page Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

    (*record).id.page = free_Page;  
    (*record).id.slot = free_Slot;

    info_table.totalRecords = info_table.totalRecords +1;

    (free_Slot == (blockfactor-1)) ? (info_table.free_space_lctn.page = free_Page+1), 
    (info_table.free_space_lctn.slot = 0) : (info_table.free_space_lctn.slot = free_Slot +1) ;    
    
  
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
    
    BM_PageHandle *page;
    page = &info_table.pageHandle;
    BM_BufferPool *bm;
    bm = &info_table.bufferPool;
    
    int page_Num_rec;
    page_Num_rec = id.page;  
    int blockfactor;
    blockfactor = info_table.blockFactor;
 

    RC pin = pinPage(bm,page,page_Num_rec);
    RC mark = markDirty(bm,page);
    RC unpin = unpinPage(bm,page);
    if(pin != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }
    
    int recordsize = info_table.recordsize;
    int recordSlotNumber = id.slot;  

    int recordOffet = recordSlotNumber * recordsize;

    memset((*page).data+recordOffet, '\0', recordsize);  
    info_table.totalRecords = info_table.totalRecords -1;  

    if(mark!=RC_OK){
        RC_message = "Page Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }
    if(unpin!=RC_OK){
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

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
    BM_PageHandle *page = &info_table.pageHandle;
    BM_BufferPool *bm = &info_table.bufferPool;
    int recordPageNumber = (*record).id.page;  
    RC a = pinPage(bm,page,recordPageNumber);
    RC b = markDirty(bm,page);
    RC c = unpinPage(bm,page);
    if(a != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }
    int recordsize = info_table.recordsize;
    int recordSlotNumber = (*record).id.slot;
    int recordOffet;
    recordOffet = recordSlotNumber * recordsize;	
    memcpy((*page).data+recordOffet, ((*record).data), recordsize-1); 

    if( b!=RC_OK){
        RC_message = "Page Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }

    if(c!=RC_OK){
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

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
    BM_PageHandle *page = &info_table.pageHandle;
    BM_BufferPool *bm = &info_table.bufferPool;
	
    int recordPageNumber = id.page; 
    if(pinPage(bm,page,recordPageNumber) != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }
    int recordsize = info_table.recordsize;
    int recordSlotNumber = id.slot;
    int recordOffet = recordSlotNumber * recordsize;  
    memcpy((*record).data, (*page).data+recordOffet, recordsize); 
    (*record).data[recordsize-1]='\0';

    (*record).id.page = recordPageNumber;
    (*record).id.slot = recordSlotNumber;

    RC a =  unpinPage(bm,page);
    if( a!=RC_OK){
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

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
    (*scan).rel = rel;
    int return_Value;
    int counter = 1;
    char point = 'y';
	
    while(counter>0)
    switch(point)
	{
		case 'n':
			return_Value = RC_FILE_NOT_FOUND;
		
		case 'y':
			scan_data.cond=cond;
			scan_data.recID.page=1; 
			scan_data.recID.slot=0;
			scan_data.count = 0;
			(*scan).mgmtData = &scan_data;
    			return RC_OK;
	}
	counter-=1;					
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
	if(scan_data.count==info_table.totalRecords)
		return RC_RM_NO_MORE_TUPLES;
    if(info_table.totalRecords < 1 )
        return  RC_RM_NO_MORE_TUPLES;
     
    int curPageScan;
    curPageScan = scan_data.recID.page;  
    int currentSlotScan;
    currentSlotScan = scan_data.recID.slot;  
    
    Value *queryExpResult = (Value *) malloc(sizeof(Value));
    scan_data.count = scan_data.count +1 ;
    
    int blockfactor = info_table.blockFactor;
    int time_record = 10;
    int scanned = 2;
    int currentTotalRecordScan = scan_data.count;
    int totalTuple = info_table.totalRecords;
    for (int counter = 0; counter<1; counter++){
		switch(scanned){
			case 1:
				while (time_record>5){
					printf("Can't write the block! ");
				}
			
			case 2:	
						
						while(currentTotalRecordScan<totalTuple)
    						{	
							scan_data.recID.page= curPageScan;
							scan_data.recID.slot= currentSlotScan;
							
							if(getRecord((*scan).rel,scan_data.recID,record) != RC_OK){
							    RC_message="Record reading failed";
							}
							currentTotalRecordScan = currentTotalRecordScan+1;  
						      

							if (scan_data.cond != NULL)
							{
							    evalExpr(record, (*((*scan).rel)).schema, scan_data.cond, &queryExpResult);
							    if((*queryExpResult).v.boolV ==1)
							    {
								(*record).id.page=curPageScan;
								(*record).id.slot=currentSlotScan;
								(currentSlotScan == (blockfactor-1))?(curPageScan =curPageScan +1),(currentSlotScan = 0):(currentSlotScan = currentSlotScan +1);
								
								scan_data.recID.page= curPageScan;
								scan_data.recID.slot= currentSlotScan;
								return RC_OK;
							    }
							}
							else
							{
							    (*queryExpResult).v.boolV = TRUE; 
							}
							
							(currentSlotScan ==(blockfactor-1)) ? curPageScan =curPageScan +1 , (currentSlotScan = 0) : (currentSlotScan = currentSlotScan +1);

						    }
						    }
						    }

    queryExpResult->v.boolV = TRUE;
    scan_data.recID.page=1; 
    scan_data.recID.slot=0; 
    scan_data.count = 0;
    return  RC_RM_NO_MORE_TUPLES; 
}



RC closeScan (RM_ScanHandle *scan)
{
    scan_data.recID.page=1;
    scan_data.recID.slot=0;
    scan_data.count = 0;
    return RC_OK;
}

/* -----------Scan Functions Ends -------------------------*/



/* -----------Schema Functions Begins -------------------------*/


int getRecordSize (Schema *schema)
{
    
    if(schema ==((Schema *)0))
    {
        RC_message = "schema is not initialized.first you need to create schema";
        return RC_SCHEMA_NOT_INIT;
    }
    int recordsize = 0;
	int i=0;
    while(i<(*schema).numAttr)
    {
		if((*schema).dataTypes[i]==DT_FLOAT){
		recordsize = recordsize + sizeof(float);
		}
		else if((*schema).dataTypes[i]==DT_BOOL){
		recordsize = recordsize + sizeof(bool);
		}
		else if((*schema).dataTypes[i]==DT_INT){
		recordsize = recordsize + sizeof(int);
		}
		else if((*schema).dataTypes[i]==DT_STRING){
		 recordsize = recordsize + (sizeof(char) * schema->typeLength[i]);
		}
    	
		i++; 
    }
    return recordsize; 
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

    Schema *schema;
    schema = (Schema *) malloc(sizeof(Schema));

        
    if(schema ==((Schema *)0)) 
    {
        RC_message = "dynamic memory allocation failed | schema";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }

    (*schema).numAttr = numAttr;
    (*schema).attrNames = attrNames;
    (*schema).dataTypes = dataTypes;
    (*schema).typeLength = typeLength;
    (*schema).keySize = keySize;
    (*schema).keyAttrs = keys;
    int recordsize = getRecordSize(schema);
    info_table.recordsize = recordsize;
    info_table.totalRecords = 0;

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
    int i=0;
	if(i==0){
		free(schema);
		return RC_OK;
	}
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

    Record *newRecord = (Record *) malloc (sizeof(Record));
    if(newRecord != ((Record *)0))
    {
		(*newRecord).data = (char *)malloc(sizeof(char) * info_table.recordsize);
    		memset((*newRecord).data,'\0',sizeof(char) * info_table.recordsize);

    		(*newRecord).id.page =-1;           
    		(*newRecord).id.page =-1;           
    		*record = newRecord;
    		return RC_OK;
     }
    else
    {
        RC_message = "dynamic memory allocation failed | Record";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }

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
    if(record == ((Record *)0))
    {
        RC_message = " Record is  null";
        return RC_NULL_IP_PARAM;
    }

    else if((*record).data != ((char *)0))
	{
        free((*record).data);
		free(record);
    return RC_OK;
	}
	else{
		return 0;
	}
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
    int offset = set_in_record(schema,attrNum);

    Value *l_rec;
    int integer_attribute;
    float float_attribute;
    int attribute_size =0;
    char *sub_string ;


    if ((*schema).dataTypes[attrNum]==DT_FLOAT)
    {
    	
            attribute_size = sizeof(float);
            sub_string= malloc(attribute_size+1);     
            memcpy(sub_string, (*record).data+offset, attribute_size);
            sub_string[attribute_size]='\0';          
            float_attribute =  atof(sub_string);
            MAKE_VALUE(*value, DT_FLOAT, float_attribute);
            free(sub_string);
	}
    else if((*schema).dataTypes[attrNum]==DT_BOOL ){
            attribute_size = sizeof(bool);
            sub_string= malloc(attribute_size+1);     
            memcpy(sub_string, (*record).data+offset, attribute_size);
            sub_string[attribute_size]='\0';         
            integer_attribute =  atoi(sub_string);
            MAKE_VALUE(*value, DT_BOOL, integer_attribute);
            free(sub_string);
		}
    else if((*schema).dataTypes[attrNum]== DT_INT ){
            attribute_size = sizeof(int);
            sub_string= malloc(attribute_size+1);     
            memcpy(sub_string, (*record).data+offset, attribute_size);
            sub_string[attribute_size]='\0';          
            integer_attribute =  atoi(sub_string);
            MAKE_VALUE(*value, DT_INT, integer_attribute);
            free(sub_string);
        }   
    else if((*schema).dataTypes[attrNum]==DT_STRING){
            attribute_size =sizeof(char)*schema->typeLength[attrNum];
            sub_string= malloc(attribute_size+1);    
            memcpy(sub_string, (*record).data+offset, attribute_size);
            sub_string[attribute_size]='\0';       // set last byet to '\0'
            MAKE_STRING_VALUE(*value, sub_string);
            free(sub_string);
        }
        
	
	return RC_OK;
	
}

void strRepInt(int j,int val,  char *intStr){
    int r=0;
    int q = val;
    int last = j;
    while (q > 0 && j >= 0) {
        r = q % 10;
        q = q / 10;
        intStr[j] = intStr[j] + r;

        j--;
    }
    intStr[last+1] = '\0';

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
    
    int remainder = 0,quotient = 0;
    int k = 0,j;
    bool q,r;
    char intStr[sizeof(int)+1];
    char intStrTemp[sizeof(int)+1];
    memset(intStr,'0',sizeof(char)*4);
    char *hexValue ="0001";
    int number;
    number = (int)strtol(hexValue, NULL, 16);
    int offset;
    offset = set_in_record(schema,attrNum);//set_in_record function returns offset of particular attribute number
    if((*schema).dataTypes[attrNum]==DT_FLOAT)
    {
        
        sprintf((*record).data + offset,"%f" ,(*value).v.floatV);
    }
    else if((*schema).dataTypes[attrNum]==DT_BOOL){
            strRepInt(1,(*value).v.boolV,intStr);
            sprintf((*record).data + offset,"%s" ,intStr);
        }
    else if((*schema).dataTypes[attrNum]==DT_INT) {
            strRepInt(3,(*value).v.intV,intStr);
            sprintf((*record).data + offset, "%s", intStr);
        }
           
    else if((*schema).dataTypes[attrNum]==DT_STRING) {
            int strLength =(*schema).typeLength[attrNum];
            sprintf((*record).data + offset, "%s", (*value).v.stringV);
           
        }
    return RC_OK;
}

/**************************************************************************************
 * Function Name: extraTotalRecordsTab
 *
 * Description: 
 *		Extract total number of records from page
 *
 * Parameters:
 *		char *schemeData : pass Scheme Data
 *		
 *				
 * Return:
 *		atoi(attributeData): total number of records
 *
 **************************************************************************************/

int extractTotalRecordsTab(char *schemaData){
    char *attributeData;
    attributeData = (char *) malloc(sizeof(char)*10);
    memset(attributeData,'\0',sizeof(char)*10);
    int i=0;
    for(i=0;schemaData[i] != '?';i++)
    {}
    i++;
    int j=0;
    do
    {
    	attributeData[j] = schemaData[i++];
    	j++;
    }while(schemaData[i] != '?');
    attributeData[j]='\0';
    return atoi(attributeData);
}


/**************************************************************************************
 * Function Name: readTotalAttributes
 *
 * Description: 
 *		Read total number of records from page
 *
 * Parameters:
 *		char *schemeData : pass Scheme Data
 *		
 *				
 * Return:
 *		atoi(strNoofAttr): convert string to int of total attributes
 *
 **************************************************************************************/
int readTotalAttributes(char *schemaData)
{
    char *strNumberOfAttribute;
    strNumberOfAttribute = (char *) malloc(sizeof(int)*2);
    char *strNoOfAttr;
    strNoOfAttr = (char *) malloc(sizeof(int)*2);
    int i = 0,j=0, cv;
    memset(strNoOfAttr,'\0',sizeof(int)*2);
    do
    {
        i++;
    } while (schemaData[i] != '|');
    
    i++;
    do
    {
        cv = j++;
        strNoOfAttr[cv]=schemaData[i++];
    }while(schemaData[i] != '[');

    strNoOfAttr[j]='\0';
    printf("\nReturn string to integer converter of total attributes");
    return atoi(strNoOfAttr);
}

/**************************************************************************************
 * Function Name: extraTotalRecordsTab
 *
 * Description: 
 *		Extract total number of key attributes from page
 *
 * Parameters:
 *		char *schemeData : pass Scheme Data
 *		
 *				
 * Return:
 *		atoi(strNoofAttr): convert string to int of key attributes
 *
 **************************************************************************************/

int keyattr_total(char *schemaData)
{
    char *strNoOfAttr;
    strNoOfAttr = (char *) malloc(sizeof(int)*2);
    memset(strNoOfAttr,'\0',sizeof(int)*2);
    int i=0,j=0;
    do
    {
        i++;
    } while (schemaData[i] != ']');
    i++;
    do
    {
        strNoOfAttr[j++]=schemaData[i++];
    }while(schemaData[i] != '{');
    strNoOfAttr[j]='\0';
    return atoi(strNoOfAttr);
}

/**************************************************************************************
 * Function Name: * dataType_getAttr
 *
 * Description: 
 *		read data type of attribute from page file
 *
 * Parameters:
 *		char *schemeData : pass Scheme Data
 *		int numAtr : number of attributes
 *				
 * Return:
 *		d_type: data type of attribute
 *
 **************************************************************************************/
int * dataType_getAttr(char *schemaData, int numAtr)
{
    int *d_type;
    d_type=(int *) malloc(sizeof(int) *numAtr);
    int i=0;
    while( i < numAtr)

    {
        char *atrDt;
        atrDt =singleattr_get_data(schemaData,i);
        d_type[i]  = dataType_extract(atrDt);
        free(atrDt);
        i++;
    }
    
    return d_type;
}



/* -----------Attribute functions ends -------------------------*/


/**************************************************************************************
 * Function Name: updateScan
 *
 * Description: 
 *		Based in the scan condition update the record
 *
 * Parameters:
 *		RM_TableData *rel: relational table
 *		Record *record: record data
 *		Record *updaterecord:  pointer to update data
 *		RM_ScanHandle *scan: other info to scan
 *				
 * Return:
 *		d_type: data type of attribute
 *
 **************************************************************************************/

RC updateScan (RM_TableData *rel, Record *record, Record *updaterecord, RM_ScanHandle *scan) 
{
    RC rc;
    if((rc = next(scan, record)) == RC_OK)
    {
        (*updaterecord).id.page=(*record).id.page;
        (*updaterecord).id.slot=(*record).id.slot;
        updateRecord(rel,updaterecord);

    }
    return RC_OK;
}


/**************************************************************************************
 * Function Name: read_Schema
 *
 * Description: 
 *		parse the data calls to further to read another parse next record
 *
 * Parameters:
 *		RM_TableData *rel: relational table and schema
 *		BM_PageHandle *h: pounter to page dat
 *				
 * Return:
 *		
 *
 **************************************************************************************/


void read_Schema(RM_TableData *rel, BM_PageHandle *h) 
{
    char metadata[PAGE_SIZE];
    strcpy(metadata,(*h).data);
    char *schema_name;
    schema_name=read_name_Schema(metadata); 
    int totalAtribute;
    totalAtribute = readTotalAttributes(metadata);
    char *atrMetadata;
    atrMetadata =metadata_Attr(metadata);
    DataType *dt;
    dt =   (DataType *)dataType_getAttr(atrMetadata,totalAtribute);
    char **names;
    names=attr_getNames(atrMetadata,totalAtribute);
    int *sizes;
    sizes = size_getAttr(atrMetadata,totalAtribute);
    char **cpNames;
    cpNames = (char **) malloc(sizeof(char*) * totalAtribute);
    int *cpSizes;
    cpSizes = (int *) malloc(sizeof(int) * totalAtribute);
    DataType *cpDt;
    cpDt = (DataType *) malloc(sizeof(DataType) * totalAtribute);
    int totalKeyAtr;
    totalKeyAtr = keyattr_total(metadata);
    char *atrKeydt;
    atrKeydt = keyattr_read_data(metadata);
    int *keys;
    keys =  key_data(atrKeydt,totalKeyAtr);
    int *cpKeys;
    cpKeys = (int *) malloc(sizeof(int)*totalKeyAtr);
    char *freeVacSlot;
    freeVacSlot = freePageSlot(metadata);
    int totaltuples;
    totaltuples = extractTotalRecordsTab(metadata);
    int *pageSlot;
    pageSlot = free_page_slot(freeVacSlot); 
    char *cpSchemaName;
    cpSchemaName = (char *) malloc(sizeof(char)*20);
    int i=0;
    memset(cpSchemaName,'\0',sizeof(char)*20); 
    while( i < totalAtribute)
    {
        cpNames[i] = (char *) malloc(sizeof(char) * 10);
        strcpy(cpNames[i], names[i]);
        i++;
    }
    memcpy(cpDt, dt, sizeof(DataType) * totalAtribute);
    memcpy(cpSizes, sizes, sizeof(int) * totalAtribute);
    memcpy(cpKeys, keys, sizeof(int) * totalKeyAtr);
    memcpy(cpSchemaName,schema_name,strlen(schema_name));
    Schema *schema;
    schema = createSchema(totalAtribute, cpNames, cpDt, cpSizes, totalKeyAtr, cpKeys);
    free(names);
    free(dt);
    free(sizes);
    free(keys);
    free(schema_name);
    (*rel).schema=schema;
    (*rel).name =cpSchemaName;
    info_table.rm_tbl_data = rel;
    info_table.recordsize =  getRecordSize((*rel).schema) + 1;  
    info_table.blockFactor = (PAGE_SIZE / info_table.recordsize);
    info_table.free_space_lctn.page =pageSlot[0];
    info_table.free_space_lctn.slot =pageSlot[1];
    info_table.totalRecords = totaltuples;

}


/**************************************************************************************
 * Function Name: *read_name_Schema
 *
 * Description: 
 *		read schema name from page file
 *
 * Parameters:
 *		char *schemaData: scheme data
 *		
 *				
 * Return:
 *		attributeData: data type of attribute
 *
 **************************************************************************************/
//function to read schema name from page file
char * read_name_Schema(char *schemaData)
{
    char *table_Name;
    table_Name = (char *) malloc(sizeof(char)*20);
    memset(table_Name,'\0',sizeof(char)*20);
    int i=0;
    do
    {
        table_Name[i]=schemaData[i];
        i++;
    } while (schemaData[i] != '|');
    table_Name[i]='\0';
    return table_Name;
}

char * metadata_Attr(char *schemaData)
{
    char *attributeData;
    attributeData = (char *) malloc(sizeof(char)*100);
    memset(attributeData,'\0',sizeof(char)*100);
    int i,j=0;
    for ( i = 0; schemaData[i] != '['; i++){
    }    
    i++;
    while(schemaData[i] != ']')
    {
        attributeData[j] = schemaData[i++];
        j++;
    }
    attributeData[j]='\0';
    return attributeData;
}

/**************************************************************************************
 * Function Name: *keyattr_read_data
 *
 * Description: 
 *		read data of attribute key from page file
 *
 * Parameters:
 *		char *schemaData: scheme data
 *		
 *				
 * Return:
 *		attributeData: data type of attribute
 *
 **************************************************************************************/

char * keyattr_read_data(char *schemaData)
{
    char *attributeData;
    attributeData = (char *) malloc(sizeof(char)*50);
    memset(attributeData,'\0',sizeof(char)*50);
    int i, j=0;
    for ( i = 0; schemaData[i] != '{'; i++){
    }    
    i++;
    do
    {
        attributeData[j] = schemaData[i++];
        j++;
    } while (schemaData[i] != '}');
    attributeData[j]='\0';
    return attributeData;
}

/**************************************************************************************
 * Function Name: *freePageSlot
 *
 * Description: 
 *		read data of free page slot from page file
 *
 * Parameters:
 *		char *schemaData: scheme data
 *		
 *				
 * Return:
 *		attributeData: data type of attribute
 *
 **************************************************************************************/

char * freePageSlot(char *schemaData){
    char *attributeData;
    attributeData = (char *) malloc(sizeof(char)*50);
    memset(attributeData,'\0',sizeof(char)*50);
    int i,j=0;
    for ( i = 0; schemaData[i] != '$'; i++){
    }
    i++;
    do
    {
        attributeData[j] = schemaData[i++];
        j++;
    } while (schemaData[i] != '$');
    attributeData[j]='\0';
    return attributeData;
}


/**************************************************************************************
 * Function Name: ** attr_getNames
 *
 * Description: 
 *		read names of attributes from page file
 *
 * Parameters:
 *		char *schemaData: scheme data
 *		int numAtr: 
 *		
 *				
 * Return:
 *		attributeNames: name of attributes that was read from page
 *
 **************************************************************************************/
char ** attr_getNames(char *schemaData, int numAtr){

    char ** attributesName;
    attributesName = (char **) malloc(sizeof(char)*numAtr);
    int i=0;
    do
    {
        char *atrDt;
        atrDt =singleattr_get_data(schemaData,i);
        char *name;
        name = name_extract(atrDt);
        attributesName[i] = malloc(sizeof(char) * strlen(name));
        strcpy(attributesName[i],name);
        i++;
        free(name);
        free(atrDt);
    }while(i<numAtr);
    return attributesName; 
}


/**************************************************************************************
 * Function Name: * size_getAttr
 *
 * Description: 
 *		read size of attribute from page
 *
 * Parameters:
 *		char *schemaData: scheme data
 *		int numAtr: 
 *		
 *				
 * Return:
 *		data_size
 *
 **************************************************************************************/

int * size_getAttr(char *schemaData, int numAtr)
{
    int *data_size= (int *) malloc(sizeof(int) *numAtr);

    int i=0;
    do
    {
        char *atrDt;
        atrDt =singleattr_get_data(schemaData,i);
        data_size[i]  = type_length(atrDt);
        i++;
        free(atrDt);
    }while(i<numAtr);

    return data_size;
}

char * singleattr_get_data(char *schemaData, int atrNum)
{
    char *attributeData;
    attributeData = (char *) malloc(sizeof(char)*30);
    int count=0;
    int i=0, j=0;
    do
    {
        if(schemaData[i++] == '(')
        {
            count++;
        }
    }while(count<=atrNum);
    do
    {
        attributeData[j] = schemaData[i++];
        j++;
    }while(schemaData[j] != ')');
    attributeData[j]='\0';
    return attributeData; 
}
 
char * name_extract(char *data)
{
    char *name;
    name = (char *) malloc(sizeof(char)*10);
    memset(name,'\0',sizeof(char)*10);
    int i=0;
    while(data[i]!=':')
    {
        name[i] = data[i];
		i++;
    }
    name[i]='\0';
    return  name; 
}

int dataType_extract(char *data)
{
    char *extData;
    extData = (char *) malloc(sizeof(int)*2);
    memset(extData,'\0',sizeof(char)*10);
    int i=0,j=0;
    while(data[i]!=':'){
		i++;
    }
    i++;
    while(data[i]!='~'){
        extData[j]=data[i++];
		j++;
    }
    
    extData[j]='\0';
    int dt;
    dt =atoi(extData);
    free(extData);
    return  dt; 
}

//function to extract length of data type
int type_length(char *data)
{
    char *datatypeLength;
    datatypeLength = (char *) malloc(sizeof(int)*2);
    memset(datatypeLength,'\0',sizeof(char)*10);
    int i=0,j=0;
	
    while(data[i]!='~'){
		i++;
    }
    i++;
    while(data[i]!='\0'){
        datatypeLength[j]=data[i++];
		j++;
    }
    
    datatypeLength[j]='\0';
    int dt; 
    dt =atoi(datatypeLength);
    free(datatypeLength);
    return  dt; // retuns the length of data type
}
//function to extract key data
int * key_data(char *data,int keyNum)
{
    char *val = (char *) malloc(sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *keyNum);
    memset(val,'\0',sizeof(int)*2);
    int i=0,j=0,k=0;
    while( data[k]!='\0')
    {
        if(data[k]!=':' )
        {
			val[i++] = data[k];

        }
        else
        {
            values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            i=0;
            j++;
        }
	 k++;
    }
    values[keyNum-1] =atoi(val);
    return  values;
}
int * free_page_slot(char *data)
{
    char *val = (char *) malloc(sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *2);
    memset(val,'\0',sizeof(int)*2);
    int i=0,j=0,k=0;
    while(data[k]!='\0')
    {
        if(data[k]!=':' )
        {
            
		val[i++] = data[k];
        }
        else
        {
            
	    values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            i=0;
            j++;
        }
		k++;
    }
    values[1] =atoi(val);
    printf("\n Slot %d",values[1]);
    return  values;
}

int set_in_record(Schema *schema, int atrnum){
    int offset;
    offset = 0;
	int pos;
    pos=0;
	
    while(pos<atrnum){
        if ((*schema).dataTypes[pos]==DT_INT){
            
                offset = offset + sizeof(int);
        }
        else if((*schema).dataTypes[pos]==DT_STRING){
                offset = offset + (sizeof(char) *  schema->typeLength[pos]);
        }
        else if((*schema).dataTypes[pos]==DT_FLOAT){
                offset = offset + sizeof(float);
        }      
        else if((*schema).dataTypes[pos]==DT_BOOL){
                offset = offset  + sizeof(bool);
                
        }
		
		pos++;
    }

    return offset;
}

//function to print record
void printRecord(char *record, int recordLength)
{
    int i=0;
    do
    {
    	printf("%c",record[i]);i++;
    }while(i<recordLength);
}


/**************************************************************************************
 * Function Name: printTableInfoDetails
 *
 * Description: 
 *		print dara of pagve
 *
 * Parameters:
 *		TableData_information *tab_info
 *		
 *				
 * Return:
 *		attributeNames: name of attributes that was read from page
 *
 **************************************************************************************/
void printTableInfoDetails(TableData_information *tab_info)
{
    printf(" \n Printing record details ");
    printf(" \n table name [%s]",(*tab_info).rm_tbl_data->name);
    printf(" \n Size of record [%d]",(*tab_info).recordsize);
    printf(" \n total Records in page (blkftr) [%d]",(*tab_info).blockFactor);
    printf(" \n total Attributes in table [%d]",(*tab_info).rm_tbl_data->schema->numAttr);
    printf(" \n total Records in table [%d]",(*tab_info).totalRecords);
    printf(" \n next available page and slot [%d:%d]",(*tab_info).free_space_lctn.page,tab_info->free_space_lctn.slot);
}

//function to print data of page
void printPageData(char *pageData)
{
    int i=0;
    printf("\n Prining page Data ==>");
    do
    {
    	printf("%c",pageData[i]);
    	i++;
    }while(i<PAGE_SIZE);
    
    printf("\n exiting ");
}
