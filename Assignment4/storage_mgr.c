#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"


FILE *file;


RC return_Value; 

void initStorageManager(void) {
	printf("\n-------------------------------Initializing Storage Manager -------------------------------");
}

RC createPageFile(char *fileName) {
	    file = fopen(fileName,"w");
	    SM_PageHandle str = malloc(PAGE_SIZE); 
	    if (str==NULL)
		return RC_WRITE_FAILED;
	    memset(str,'\0',PAGE_SIZE); 
	    
	    fprintf(file,"%d\n",1); // give the pages as 1
	    fwrite(str, sizeof(char), PAGE_SIZE, file);
	    fclose(file);
	    
	    free(str);
	    str=NULL;
	    return RC_OK;

}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
	    file = fopen(fileName,"r+");
	    if (file==NULL){
		return RC_FILE_NOT_FOUND;
	    
	}
		else
		{
	    int tot_pg;
	    fscanf(file,"%d\n",&tot_pg);
	    
	    (*fHandle).fileName=fileName;
	    (*fHandle).totalNumPages=tot_pg; 
	    (*fHandle).curPagePos=0;  
	    (*fHandle).mgmtInfo=file; 
	    return RC_OK;
	}
}	


RC closePageFile(SM_FileHandle *fHandle) {
	int no_Page =fclose((*fHandle).mgmtInfo); 
    	if (no_Page!=0){
			return RC_FILE_HANDLE_NOT_INIT;
		}
		else{
        	return RC_OK;
        }
}

RC destroyPageFile(char *fileName) {
	return_Value = remove(fileName) !=0 ? RC_FILE_NOT_FOUND:RC_OK;
	return return_Value;
	}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	if (pageNum > (*fHandle).totalNumPages){
        	return RC_READ_NON_EXISTING_PAGE;
	}
	else if( pageNum<0){
		return RC_READ_NON_EXISTING_PAGE;
	}
	else if((*fHandle).mgmtInfo != NULL){
		fseek((*fHandle).mgmtInfo, 5+pageNum*PAGE_SIZE, SEEK_SET);
    	fread(memPage, sizeof(char), PAGE_SIZE, (*fHandle).mgmtInfo); 
    	(*fHandle).curPagePos = pageNum;
    return RC_OK;
	}
	else{
		return RC_OK;
	}
	
}



int getBlockPos(SM_FileHandle *fHandle)
{
    
    return_Value = (fHandle != NULL) ? fHandle->curPagePos : RC_FILE_NOT_FOUND;
    return return_Value;
}


RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{

  return_Value == (RC_OK == readBlock(0, fHandle, memPage)) ? RC_OK : RC_READ_NON_EXISTING_PAGE; 
  return return_Value;
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	
    return_Value == (RC_OK == readBlock(getBlockPos(fHandle)-1, fHandle, memPage)) ? RC_OK : RC_READ_NON_EXISTING_PAGE;
    return return_Value;
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
	
    return_Value == (RC_OK == readBlock(getBlockPos(fHandle),fHandle,memPage)) ? RC_OK : RC_READ_NON_EXISTING_PAGE;
    return return_Value;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
	
    return_Value == (RC_OK == readBlock(getBlockPos(fHandle)+1, fHandle, memPage)) ? RC_OK : RC_READ_NON_EXISTING_PAGE;                             
    return return_Value;
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
	
    return_Value == (RC_OK == readBlock((fHandle->totalNumPages-1), fHandle, memPage)) ? RC_OK : RC_READ_NON_EXISTING_PAGE; 
    return return_Value;
}


RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    
	
	if (pageNum > (fHandle->totalNumPages) || pageNum<0)
        return RC_WRITE_FAILED;
    
	int no_page = fseek(fHandle->mgmtInfo, 5+pageNum*PAGE_SIZE, SEEK_SET);
	if (no_page)
		return RC_WRITE_FAILED;
	    
	fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
	fHandle->curPagePos = pageNum;
	return RC_OK;
	
}


RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC cur_block_w;

	cur_block_w = writeBlock((*fHandle).curPagePos, fHandle, memPage);
	cur_block_w == RC_OK ? RC_OK : RC_WRITE_FAILED;

}
	

RC appendEmptyBlock(SM_FileHandle *fHandle) {
	
	SM_PageHandle str = (char *) calloc(PAGE_SIZE,1); 
	RC return_value = writeBlock(fHandle->totalNumPages, fHandle, str);
	int counter = 1;
	char point = 'y';
	int return_Value =0;
	while(counter>0){
	switch(point)
	{
		case 'n':
			return return_Value = RC_WRITE_FAILED;
		
		case 'y':
			
		    	
		    	if(return_value==RC_OK){
				fHandle->curPagePos=fHandle->totalNumPages;
			fHandle->totalNumPages+=1;
			    
			rewind(fHandle->mgmtInfo);
			fprintf(fHandle->mgmtInfo,"%d\n",fHandle->totalNumPages);
			fseek(fHandle->mgmtInfo,5+(fHandle->curPagePos)*PAGE_SIZE,SEEK_SET); 
			free(str);
			str=NULL;
			return RC_OK;
			}
			else{
				free(str);
				str=NULL;
				return return_value;
	}	
	}
	counter-=1;
		
}
}


RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {

	if ((*fHandle).totalNumPages > numberOfPages)
   	{
		return RC_OK;
		
	}
	else{
		int difference = numberOfPages - fHandle->totalNumPages;
		RC return_value;
		int i=0; 
		while(i < difference)
		{
		    return_value = appendEmptyBlock(fHandle);
		    if (return_value!=RC_OK)
		        return return_value;
			i++;
		}
		
    }
   
 }
/* ---------------------Functions to write blocks to page file ends----------------------------------- */
