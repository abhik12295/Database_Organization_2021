#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"


typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  void *mgmtData;
} RM_ScanHandle;


typedef struct TableData_information{
    int recordsize; 
    int totalRecords; 
    int blockFactor;
    RID free_space_lctn; 
    RM_TableData *rm_tbl_data;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;
}TableData_information;

typedef struct RM_SCANDATA_MANAGEMENT{
    RID recID; 
    Expr *cond; 
    int count;  
    RM_TableData *rm_tbl_data;
    BM_PageHandle rm_pageHandle;
    BM_BufferPool rm_bufferPool;
}RM_SCANDATA_MANAGEMENT;



void read_Schema(RM_TableData *, BM_PageHandle *);
char * read_name_Schema(char *);
char * metadata_Attr(char *);
int keyattr_total(char *);
char * keyattr_read_data(char *);
char * singleattr_get_data(char *, int );
char ** attr_getNames(char *, int );
char * name_extract(char *);
int dataType_extract(char *);
int * dataType_getAttr(char *, int );
int type_length(char *data);
int * size_getAttr(char *schemaData, int numAtr);
int * key_data(char *data,int keyNum);
int * free_page_slot(char *);
char * freePageSlot(char *);
int set_in_record(Schema *, int );

// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);
extern RC updateScan (RM_TableData *rel, Record *record,Record *updaterecord, RM_ScanHandle *scan);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

#endif // RECORD_MGR_H
