#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

FILE *file;

int init = 0;
RC return_Value; //return_Value
int checkinit(void){
	return_Value = (init == 1)? RC_OK: -1;
	return return_Value;
	}

void initStorageManager(void) {
	
	
	printf("\n Initializing Storage Manager \n ");
	if(checkinit() != RC_OK)
	{
		init=1;
			printf("\n Storage Manager Initialized\n");
	}
	else{
		printf("\n Storage Manager has already been Initialized\n");
	}
	
}

//Creating Page file
RC createPageFile(char *fileName) {
	//char *memoryBlock = malloc(PAGE_SIZE * sizeof(char)); 
	int write_Page;
	RC return_Value;
	int i=0;
	file = fopen(fileName, "w+"); 
	
	if (file == NULL)
	{
		return RC_FILE_NOT_FOUND;
	}
	else
	{		
		while(i < PAGE_SIZE){
			fwrite("\0", 1,1, file);
			fseek(file,0,SEEK_END);
			i++;
		}
		(write_Page >= PAGE_SIZE) ? printf("\nWrite Operation SUCCESSFUL.\n"):printf("\nWrite Operation FAILED.\n");
		//free(memoryBlock);		
		fclose(file);			
		return_Value = RC_OK; 
	}	
	return return_Value;

}

//Opening Page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
	file = fopen(fileName, "r+");
	if (file == NULL)
	{
		return RC_FILE_NOT_FOUND;
	}
	else
	{
		fseek(file, 0, SEEK_END);
		int EndByte = ftell(file); 		//ftell returns the last byte of file
		int TotalLength = EndByte + 1;
		int totalNumPages = TotalLength / PAGE_SIZE; 
		
		
		fHandle -> fileName= fileName;
		fHandle -> totalNumPages = totalNumPages;
		fHandle -> curPagePos = 0;
		rewind(file); 
		return RC_OK;
	}	
}

//Closing Page file
RC closePageFile(SM_FileHandle *fHandle) {
	RC isFileClosed;
	isFileClosed = fclose(file);//fclose closes the file successfully and returns 0
	if (isFileClosed != 0)
		return RC_FILE_NOT_FOUND;
	else
		return RC_OK;
}

//Destroying Page file
RC destroyPageFile(char *fileName) {
	return_Value = remove(fileName) !=0 ? RC_FILE_NOT_FOUND:RC_OK;
	return return_Value;
	}





//Reading pageNum block from the file
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int read_block;

	if (pageNum > (fHandle->totalNumPages))	
		return_Value = RC_READ_NON_EXISTING_PAGE;
		

	else {
		fseek(file, pageNum * PAGE_SIZE, SEEK_SET); 
		read_block = fread(memPage, sizeof(char), PAGE_SIZE, file);
		if (read_block < PAGE_SIZE || read_block > PAGE_SIZE)
		{
			return_Value = RC_READ_NON_EXISTING_PAGE;
		}
		fHandle->curPagePos = pageNum;
		return_Value = RC_OK;
	}

	return return_Value;
		
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


//Writing block at page pageNum
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_Value;
	int time_stamp_1 = 1;
	int time_stamp = 10;
	int ch = 2;
	for (int counter = 0; counter<1; counter++){
		switch(ch){
			case 1:
				while (time_stamp>5){
					printf("Can't write the block! ");
				}
			
			case 2:
				if (pageNum < 0 || pageNum > (*fHandle).totalNumPages)
					return_Value = RC_WRITE_FAILED;
				else
				{
					while (time_stamp_1 > 0){
						if(fHandle == NULL){
							return_Value = RC_FILE_NOT_FOUND;
						}
						else{
						
							if (file != NULL) 
								{ 
									if (fseek(file, (PAGE_SIZE * pageNum), SEEK_SET) == 0) 
									{
										fwrite(memPage, sizeof(char), PAGE_SIZE, file);
										fHandle->curPagePos = pageNum; 
										fseek(file, 0, SEEK_END);
										fHandle->totalNumPages = ftell(file) / PAGE_SIZE;
										return_Value = RC_OK;
									}
									else 
										return_Value = RC_WRITE_FAILED;
								} 
							else 
								return_Value = RC_FILE_NOT_FOUND;
						}
						time_stamp_1-=1;
					}			
				}
	}
	return return_Value;
}
}

//Writing current block in the file
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC cur_pos;
	RC cur_block_w;

	cur_pos = (fHandle->curPagePos/PAGE_SIZE) + 1; 
	cur_block_w = writeBlock(cur_pos, fHandle, memPage);
	cur_block_w == RC_OK ? RC_OK : RC_WRITE_FAILED;
}
	
//Append empty block to the file
RC appendEmptyBlock(SM_FileHandle *fHandle) {
	
	int return_Value;
	int counter = 1;
	char point = 'y';
	
	while(counter>0)
	switch(point)
	{
		case 'n':
			return return_Value = RC_WRITE_FAILED;
		
		case 'y':
			if(file == NULL)
				{
					return_Value = RC_FILE_NOT_FOUND;
				}
			
			else
			{
				RC size = 0;
				char *new_block;
				new_block = (char *) calloc(PAGE_SIZE, sizeof(char));
				
				fseek(file, 0, SEEK_END);
				size= fwrite(new_block, 1, PAGE_SIZE, file);
				
				if (size != PAGE_SIZE)
				{
					return_Value = RC_WRITE_FAILED;
					free(new_block);
				}
				
				
				else 
				{
					fHandle->totalNumPages = ftell(file) / PAGE_SIZE;
					fHandle->curPagePos = fHandle->totalNumPages - 1;
					return_Value = RC_OK;
				}
				
			return return_Value;		
			}		
	}
	counter-=1;
}


//Ensure capacity of the file is numberOfPages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
	
	int page_point = fHandle->totalNumPages;
	int page_cap = numberOfPages - page_point ;                
	
	if(page_cap < 0)                                                         
	{
		return_Value = RC_WRITE_FAILED;
		                                                
	}
	else
	{
		for (int count=0; count < page_cap; count++)
			appendEmptyBlock(fHandle);                                   
		return_Value = RC_OK; 
	}

    return return_Value;
}
/* --------------------------------methods to write blocks to page file ends----------------------------------------------------------------------------------------- */
