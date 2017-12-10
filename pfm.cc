#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	const char* name = fileName.c_str();
	FILE *file = fopen(name, "r");
	// file already exists:
	if(file != NULL) {
		fclose(file);
		return -1;
	}

	file = fopen(name, "w");

	if(file == NULL) return -1;

	fclose(file);
	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	const char* name = fileName.c_str();
	FILE *file = fopen(name, "r");
	if(file == NULL){
		return -1;
	}

    return remove(name);
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	const char* name = fileName.c_str();
	FILE *file = fopen(name, "r+");
	if(file == NULL) {
		return -1;
	}
	fileHandle.file = file;
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{

    fileHandle.close();
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    file = NULL;

}


FileHandle::~FileHandle()
{
}


bool FileHandle::alreadyOpen()
{
	return file != NULL;
}


void FileHandle::close()
{
	fclose(file);
	file = NULL;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
	// check if file is already closed or the pageNum is too big;
	if( !alreadyOpen() || pageNum >= getNumberOfPages()){
		 return -1;
	}
	// check whether fseek and fread would fail.
	int fs = fseek(file, PAGE_SIZE * pageNum, SEEK_SET);
	if(fs != 0) {
		return -1;
	}

	fread(data, 1, PAGE_SIZE, file);

	readPageCounter += 1;
	rewind(file);
	return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	// check if file is already closed or the pageNum is too big;
	if( !alreadyOpen() || pageNum >= getNumberOfPages()){
		 return -1;
	}

	// check whether fseek and fwrite would fail.
	int fs = fseek(file, PAGE_SIZE * pageNum, SEEK_SET);
	if(fs != 0) return -1;

	fwrite(data, 1, PAGE_SIZE, file);

	writePageCounter += 1;
	fflush(file);
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	if(!alreadyOpen()) return -1;

	fseek(file, 0, SEEK_END);
	int fw = fwrite(data, 1, PAGE_SIZE, file);

	if(fw != PAGE_SIZE) {
		fflush(file);
		return -1;
	}

	appendPageCounter += 1;
	fflush(file);
	return 0;

}


unsigned FileHandle::getNumberOfPages()
{
	fseek(file, 0, SEEK_END);
	unsigned long num = ftell(file)/PAGE_SIZE;
	return num;
}



RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return 0;
}
