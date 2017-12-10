
#include <stack>
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}
RC IndexManager::createFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->createFile(fileName);
    return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->destroyFile(fileName);
    return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    //check again how the fileHandle open and close
	if(ixfileHandle.alreadyOpen()) return -1;
	PagedFileManager *pfm = PagedFileManager::instance();
	FileHandle fileHandle;
	RC rc = pfm->openFile(fileName, fileHandle);
	ixfileHandle.fileHandle = fileHandle;
	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	if(! ixfileHandle.alreadyOpen()) return -1;
	PagedFileManager *pfm = PagedFileManager::instance();
	RC rc = pfm->closeFile(ixfileHandle.fileHandle);

    return rc;
}
RC initialMD(FileHandle &fileHandle) {
	void *buffer = malloc(PAGE_SIZE);
	unsigned int firstPage = 1;

	memcpy(buffer, &firstPage, RS); //Meta data page to indicate the root pageNum using first short.
	RC rc = fileHandle.appendPage(buffer);
	free(buffer);
	return rc;
}

RC firstLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute,const void *key, unsigned int &keyLen, const RID &rid){
	if(ixfileHandle.fileHandle.getNumberOfPages() > 0) return -1;
	initialMD(ixfileHandle.fileHandle);
	void *root = malloc(PAGE_SIZE);
	short parent = -1;
	short total = 1;
	unsigned int next = redirected;
	memcpy(root, &parent, 2);//set ParentNum to -1
	memcpy((char *)root+2, &total, 2);//total keys == 1(short)
	memcpy((char *)root+4, &total, 2); //is leaf
	memcpy((char *)root+6, &next, RS);//next is empty

	//set the first key entry in the root
	keyLen = attribute.type==TypeVarChar ? *(int *)((char *) key) + 4 : 4;

	memcpy((char *)root+6 + RS, key, keyLen);
	memcpy((char *)root+6 + RS +keyLen, &rid.pageNum, RS);
	memcpy((char *)root+6 + RS+keyLen + RS, &rid.slotNum, RS);

	short freeSpaceOffset = 6 + 3*RS + keyLen;
	memcpy((char *)root+PAGE_SIZE-2, &freeSpaceOffset, 2);
	RC rc = ixfileHandle.fileHandle.appendPage(root);

	free(root);
	return rc;
}

unsigned int getRootPage(FileHandle & fileHandle) {

	void *buffer = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(0, buffer);
	if(rc < 0) {
		free(buffer);
		return redirected;
	}
	unsigned int pageNum = *(unsigned int *)buffer;
	free(buffer);
	return pageNum;
}

void getDirectory(void *page, short &parent, short &totalKeys, short &isLeaf, unsigned int &rightSibling, short &freeSpaceOffset, bool & doubled){
	parent = *(short *)page;
	totalKeys = *(short *)((char *)page+2);
	isLeaf = *(short *)((char *)page+4);
	rightSibling = *(unsigned int *)((char *)page+6);
	if(doubled){
		freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE + PAGE_SIZE-2);
	}else{
		freeSpaceOffset = *(short *)((char *)page +PAGE_SIZE-2);
	}

}

void writeDirectory(void *page, const short & parent, const short & totalKeys, const short & isLeaf, const unsigned int & rightSibling, const short &freeSpaceOffset) {

	memcpy(page, &parent, 2);
	memcpy((char *)page + 2, &totalKeys, 2);
	memcpy((char *)page + 4, &isLeaf, 2);
	memcpy((char *)page + 6, &rightSibling, RS);
	memcpy((char *)page + PAGE_SIZE - 2, &freeSpaceOffset, 2);

}

bool isEmpty(const void *buffer, const short &offset){
 	int *temp = (int *)((char *)buffer + offset);
 	if(temp == nullptr) return true;
 	return false;
 }

// that is to compare the key with the currentValues in the given page with proper offset:
int compareWithKey(const void *currentPage, const short &offset, const void *key, const Attribute &attribute){
	// -1:key is larger
	// 0: key is equal
	// 1 :key is smaller, this is the right place
	int keyValue, currentValue;
	float keyValue1, currentValue1;
	int keyLen, currentLen;
	if(isEmpty(currentPage, offset)){
		return -200;
	}
	switch(attribute.type){
	case TypeInt:
		keyValue = *(int *)key;
		currentValue = *(int *)((char *)currentPage + offset);
		if(keyValue < currentValue){
			return 1;
		}
		else if(keyValue == currentValue ){
			//	isExist = true;
				return 0;
		}else{
			return -1;
		}

	case TypeReal:
		keyValue1 = *(float *)(char *)key;
		currentValue1 = *(float *)((char *)currentPage + offset);

//		cout << "keyValue1 " << keyValue1 << endl;
//		cout << "currentValue " << currentValue1 << endl;
		if(keyValue1 < currentValue1){
			return 1;
		}else if(keyValue1 == currentValue1){
		//		isExist = true;
				return 0;
		}else{
			return -1;
		}
	case TypeVarChar:
		keyLen = *(int *)key;
		currentLen = *(int *)((char *)currentPage+offset);
		//cout<<"key len"<<keyLen <<"   current len "<<currentLen<<endl;
		string keyValue2;
		for (int i=0;i<keyLen; i++) keyValue2.push_back(*(char *)((char *)key + i + 4));
		string currentValue2;
		for (int i=0; i<currentLen; i++) currentValue2.push_back(*(char *)((char *)currentPage+offset+i + 4));
		//cout<<keyValue2<<"       "<<currentValue2<<endl;
		if(strcmp(keyValue2.c_str(),currentValue2.c_str())==0){
		//	isExist=true;
			return 0;
		}
		return strcmp(currentValue2.c_str(),keyValue2.c_str());
	}
	return -200;
}



unsigned int getKeyOffsetInParent(void *parentNode, const short &totalKeys, const void *key, const Attribute &attribute ){
	//return the offset, which is the position of the child, in which we want to insert key
	int res = -1;
	unsigned int result = redirected;
	//int keyLen = attribute.type==TypeVarChar ? *((int *) key)+4 : 4;
	int i=0;
	short offset = 6 + 2*RS; // 10 + 4;
	for(; i< totalKeys; i++){
		res = compareWithKey(parentNode, offset, key, attribute);
		//cout<<"the result of compare is "<<res<<endl;
		if(res == -200) return redirected;
		if(res>0){  // key should be inserted into left child, key is smaller than the parent offset:

			result = *(unsigned int *)((char *)parentNode + offset - RS);
			break;

		}else{

			int len = attribute.type == TypeVarChar ? *(int *)((char *)parentNode + offset) + 4 : 4;
			offset += (RS + len);
			//continue;
		}
	}
	//cout<<" 214 result "<<result<<endl;
	if(i==totalKeys){
		result = *(unsigned int *)((char *)parentNode + offset - RS);
		//we searched through all the entries and insert into right child of last key;

//		short freeSpaceStart = *(short *)((char *)parentNode+PAGE_SIZE-2);
//		if(freeSpaceStart != offset) return -1;
	}
//	offset -= 2;//so now the offset is for the page number of child
//	if(equal){
//		offset += 2 + keyLen;
//	}
	return result;
}


RC getLeafNode(FileHandle & fileHandle, const Attribute &attribute, const void *key, stack<unsigned int> &internalStack){
	if(!fileHandle.alreadyOpen()){
		return -1;
	}
	unsigned int rootPage = getRootPage(fileHandle);
	if(rootPage == redirected) {
		return -1; //fail to read meta data page
	}
	void *buffer = malloc(PAGE_SIZE);
	//RC rc = fileHandle.readPage(rootPage, buffer);
	//short isLeaf = *(short *)((char *)buffer + 4);
	while(true){
		internalStack.push(rootPage);
		//buffer = malloc(PAGE_SIZE);
		PageNum rP = rootPage;
		RC rc = fileHandle.readPage(rP, buffer);

		if(rc < 0) {
			//internalStack.clear();
			free(buffer);
			return -1;
		}

		short isLeaf = *(short *)((char *)buffer + 4);
		if(isLeaf){
			break;
		}
		short totalKeys = *(short *)((char *)buffer + 2);
		rootPage = getKeyOffsetInParent(buffer, totalKeys, key, attribute);

		if(rootPage == redirected) {
			return -1;
		}

	}
	free(buffer);
	//find the parent and the leaf
	return 0;
}

RC splitInternalNode(void* buffer, const Attribute & attribute, void *upperKey, const unsigned int &c_pageNum, unsigned int & r_pageNum, FileHandle &fileHandle){
	void * newPage = malloc(PAGE_SIZE);
	short isRoot, totalKeys, isLeaf, offset;
	unsigned int nextPage;
	bool doubled = true;
	getDirectory(buffer, isRoot, totalKeys, isLeaf, nextPage, offset, doubled);
	int start = 6 + RS*2;
	int keyLen = -1;
	short keyNum = totalKeys/2;
	for(int i = 0; i < keyNum; i++) {
		keyLen = attribute.type == TypeVarChar ? *(int *)((char *)buffer + start) + 4 : 4;
		start += (keyLen + RS);
	}
	keyLen = attribute.type == TypeVarChar ? *(int *)((char *)buffer + start) + 4 : 4;

	memcpy(upperKey, (char *)buffer + start, keyLen); // this is to record the key to be transferred to upper level;
	start += keyLen;
	memmove((char *)newPage + 6 + RS, (char *)buffer + start, offset - start);
	if(isRoot == -1){
		//if this is rootPAGE, change it to a non-root page;
		isRoot = 0;
	}

	short r_keyNum = totalKeys - keyNum - 1;
	short r_offset = offset - start + 6 + RS;
	writeDirectory(newPage, isRoot, r_keyNum, isLeaf, nextPage, r_offset);
	fileHandle.appendPage(newPage);
	r_pageNum = fileHandle.getNumberOfPages() - 1;
	start -= keyLen;
	writeDirectory(buffer, isRoot, keyNum, isLeaf, r_pageNum, start);
	//fileHandle.writePage(c_pageNum, buffer);
	free(newPage);
	return 0;
}

RC buildRoot(void *page, const Attribute& attribute, void *key, unsigned int &leftLeaf, unsigned int &rightLeaf){
	short isRoot = -1,  totalKeys = 1, isLeaf = 0;
	unsigned int nextPage = redirected;
	int keyLen =  attribute.type == TypeVarChar ? *(int *)((char *)key) + 4 : 4;
	short offset = 6 +RS +RS + keyLen + RS;
	writeDirectory(page, isRoot, totalKeys, isLeaf, nextPage, offset);
	memcpy((char *)page + 6 +RS , &leftLeaf, RS);
	memcpy((char *)page + 6 + RS +RS, key, keyLen);
	memcpy((char *)page + 6 +RS +RS + keyLen, &rightLeaf, RS);
	return 0;
}

RC insertInternalNode(FileHandle & fileHandle, const Attribute & attribute, unsigned int & pageNum, unsigned int &leftLeaf,
		unsigned int &rightLeaf, void *key, stack<unsigned int> &parentStack) {

	if(!fileHandle.alreadyOpen()){
		return -1;
	}

	if(pageNum == redirected){
		void *page = malloc(PAGE_SIZE);
		buildRoot(page, attribute,key, leftLeaf, rightLeaf);
		fileHandle.appendPage(page);
		free(page);
		unsigned int newRoot = fileHandle.getNumberOfPages() - 1;
		void *md = malloc(PAGE_SIZE);
		memcpy(md, &newRoot, RS);
		RC rc = fileHandle.writePage(0, md);
		free(md);
		return rc;
	}
	void *buffer = malloc(PAGE_SIZE + PAGE_SIZE);
	//short l_pageNum = pageNum;
	PageNum pageNumber = pageNum;
	fileHandle.readPage(pageNumber, buffer);
	short isRoot, totalKeys, isLeaf, offset;
	unsigned int nextPage;
	bool doubled = false;
	getDirectory(buffer, isRoot, totalKeys, isLeaf, nextPage, offset, doubled);
	/***********need check *******/
	unsigned int parent = redirected; // if this page is root;
	if(! parentStack.empty()){
		parent = parentStack.top();
		parentStack.pop();
	}
	bool split = false;
	int recordLen = attribute.type == TypeVarChar ? *(int *)((char *)key) + 4 : 4;
	if(offset + recordLen + RS + 2 > PAGE_SIZE) {
		split = true;
	}
	int start = 6 + 2*RS;
	int i = 0;
	int res = -1;
	for(; i< totalKeys; i++){
		res = compareWithKey(buffer, start, key, attribute);
		if(res == -200) return -1;
		//THINK HERE if we can have a key which is equal to the original key in internal node?
		if(res>0){  // key should be inserted into left child.
			//	result = *(short *)((char *)buffer + start - 2);
			break;
		}else{
				int len = attribute.type == TypeVarChar ? *(int *)((char *)buffer + start) + 4 : 4;
				start += (RS + len);
				//continue;
		}
	}

	memmove((char *)buffer + start + recordLen + RS, (char *)buffer + start, offset - start);
	memcpy((char *)buffer + start, key, recordLen);
	memcpy((char *)buffer + start + recordLen, &rightLeaf, RS);
	totalKeys++;
	offset += (RS + recordLen);
	memcpy((char *)buffer + 2, &totalKeys, 2); //update totalKeys;

	if(split) {
		memcpy((char *)buffer + PAGE_SIZE + PAGE_SIZE - 2, &offset, 2);
		unsigned int r_pageNum;
		void *upperKey = malloc(attribute.length+4);
		//void *newPage = malloc(PAGE_SIZE);
		splitInternalNode(buffer, attribute, upperKey, pageNum, r_pageNum, fileHandle);
		PageNum pageNumber = pageNum;
		fileHandle.writePage(pageNumber, buffer);
		free(buffer);
		//insertInternalNode(fileHandle, attribute, parent, pageNum, r_pageNum, upperKey, parentStack);

		insertInternalNode(fileHandle, attribute, parent, pageNum, r_pageNum, upperKey, parentStack);
		free(upperKey);
	}
	else{
		memcpy((char *)buffer + PAGE_SIZE - 2, &offset, 2);
		PageNum pageNumber = pageNum;
		fileHandle.writePage(pageNumber, buffer);
		free(buffer);
	}

	return 0;

}

RC insertDuplicate(FileHandle &fileHandle,
		const Attribute &attribute,
		void *buffer, const short &offset, const unsigned int &pageNum,
		const void *key, const short &keyLen, const RID &rid){
	//int duplicateEntryLen  = attribute.type==TypeVarChar ? *(int *)((char *)buffer + offset)+4 : 4; //same as keyLen
	unsigned int duplicatePageNum = *(unsigned int *)((char *)buffer+offset+keyLen);
	unsigned int duplicateSlotNum = *(unsigned int *)((char *)buffer+offset+keyLen+RS);
	short totalEntries = *(short *)((char *)buffer + 2);
	if(duplicatePageNum == redirected){
	//there is some overflow page for this key
		unsigned int overflowPageNum = duplicateSlotNum;
		void *overflowPage = malloc(PAGE_SIZE);
		fileHandle.readPage(overflowPageNum, overflowPage);
		short freeSpaceStartAtOP = *(short *)((char *)overflowPage+PAGE_SIZE - 2- RS);
		if(freeSpaceStartAtOP + 2*RS + RS + 2 <= PAGE_SIZE){//the first overflow page is not full
			//insert to this page
			memcpy((char *)overflowPage + freeSpaceStartAtOP, &rid.pageNum , RS);
			memcpy((char *)overflowPage + freeSpaceStartAtOP + RS, &rid.slotNum , RS);
			//nothing for the leaf page to be changed, but to update the directory of overflow page
			freeSpaceStartAtOP += RS*2;
			memcpy((char *)overflowPage + PAGE_SIZE - RS -2, &freeSpaceStartAtOP, 2);
			RC rc = fileHandle.writePage(overflowPageNum,overflowPage);

			totalEntries += 1;
			memcpy((char *)buffer+2, &totalEntries, 2);
			fileHandle.writePage(pageNum,buffer);
			free(buffer);
			free(overflowPage);
			return rc;
		}else{//the first overflow page is full
			unsigned int nextOFPNum = *(unsigned int *)((char *)overflowPage +PAGE_SIZE-RS);
			unsigned int currentPage = overflowPageNum;
			while(nextOFPNum != redirected){
			//read next overflow page and check for insertion
				fileHandle.readPage(nextOFPNum, overflowPage);
				freeSpaceStartAtOP = *(short *)((char *)overflowPage + PAGE_SIZE - RS -2);
				if(freeSpaceStartAtOP + 2*RS + RS + 2 <= PAGE_SIZE){
					break;
				}
				currentPage  = nextOFPNum;
				nextOFPNum = *(unsigned int *)((char *)overflowPage +PAGE_SIZE-RS);
			}

			if(nextOFPNum == redirected){
				//the last overflow page is full, we need a new overflow page
				void *newOverflowPage = malloc(PAGE_SIZE);
				memcpy(newOverflowPage, &rid.pageNum, RS);
				memcpy((char *)newOverflowPage + RS, &rid.slotNum, RS);
				unsigned int flag = redirected;
				memcpy((char *)newOverflowPage+PAGE_SIZE-RS,&flag, RS);//next pointer
				short temp = 2*RS;
				memcpy((char *)newOverflowPage+PAGE_SIZE-RS-2, &temp, 2);//free space start offset
				RC rc1 = fileHandle.appendPage(newOverflowPage);

				totalEntries += 1;
				memcpy((char *)buffer+2, &totalEntries, 2);
				fileHandle.writePage(pageNum,buffer);

				unsigned int newNextPageNum = fileHandle.getNumberOfPages()-1;
				memcpy((char *)overflowPage+PAGE_SIZE - RS, &newNextPageNum, RS);
				fileHandle.writePage(currentPage,overflowPage);

				free(buffer);
				free(overflowPage);
				free(newOverflowPage);
				return rc1;

			} else{
				//the current overflow page is not full, we can insert to it
				memcpy((char *)overflowPage+freeSpaceStartAtOP, &rid.pageNum, RS);
				memcpy((char *)overflowPage+freeSpaceStartAtOP + RS, &rid.slotNum, RS);

				freeSpaceStartAtOP += 2*RS;
				memcpy((char *)overflowPage+PAGE_SIZE - RS - 2, &freeSpaceStartAtOP, 2);
				RC rc2 = fileHandle.writePage(nextOFPNum,overflowPage);
				totalEntries += 1;
				memcpy((char *)buffer+2, &totalEntries, 2);
				fileHandle.writePage(pageNum,buffer);
				free(buffer);
				free(overflowPage);
				return rc2;
			}
		}
	}else{
		//there is no overflow page, we need create one
		void *firstOverflowPage = malloc(PAGE_SIZE);
		memcpy(firstOverflowPage,&duplicatePageNum, RS);
		memcpy((char *)firstOverflowPage + RS, &duplicateSlotNum, RS);
		memcpy((char *)firstOverflowPage + RS*2, &rid.pageNum, RS);
		memcpy((char *)firstOverflowPage + RS*3, &rid.slotNum, RS);
		unsigned int flag1 = redirected;
		memcpy((char *)firstOverflowPage+PAGE_SIZE-RS, &flag1, RS);
		short flag2 = RS*4;
		memcpy((char *)firstOverflowPage+PAGE_SIZE-RS -2, &flag2,2);

		fileHandle.appendPage(firstOverflowPage);

				//update the slot in leaf
		unsigned int flag = redirected;
		memcpy((char *)buffer+offset+keyLen, &flag, RS);
		unsigned int firstOverflowPageNum = fileHandle.getNumberOfPages()-1;
		memcpy((char *)buffer+offset+keyLen+RS, &firstOverflowPageNum,RS);
		totalEntries += 1;
		memcpy((char *)buffer+2, &totalEntries, 2);
		RC rc3 = fileHandle.writePage(pageNum,buffer);
		free(buffer);
		free(firstOverflowPage);
		return rc3;
	}
}
int getOFNum(FileHandle &fileHandle, unsigned int pageNum){
	int cnt = 0;
	void *buffer = malloc(PAGE_SIZE);
	while(pageNum != redirected){
		RC rc = fileHandle.readPage(pageNum, buffer);
		if(rc < 0) return -1;
		short offset = *(short *)((char *)buffer + PAGE_SIZE - RS -2);
		cnt += offset/(2*RS);
		pageNum = *(unsigned int *)((char *)buffer + PAGE_SIZE - RS);
	}
	free(buffer);
	return cnt;
}

RC insertToLeaf(FileHandle &fileHandle,
		const Attribute &attribute,
		const unsigned int & pageNum,//the page number of the leaf
		const void* key,
		const RID &rid,
		void *midEntry,
		unsigned int &rightChildPageNum,
		bool &updateParent){
	void *buffer = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(pageNum,buffer);
	if(rc<0) {
		free(buffer);
		return -1;
	}
	unsigned int rightSibling = redirected;
	short parentNum, totalEntries, isLeaf, freeSpaceOffset;
	bool doubled = false;
	getDirectory(buffer,parentNum, totalEntries, isLeaf, rightSibling, freeSpaceOffset, doubled);
	int keyLen = attribute.type==TypeVarChar ?  *(int *)((char *)key) + 4 : 4;
	short offset = 6 + RS;
	bool isExist = false;
	//find the offset, which is the right place to insert the key
	for (int cursor=6+RS; cursor<freeSpaceOffset;){
		int res= compareWithKey(buffer, offset, key, attribute);
		if(res>=0){
			if(res==0) isExist=true;
			break;
		}else {
			int len = attribute.type==TypeVarChar ? *(int *)((char *)buffer + offset) + 4 + RS*2 : 4 + RS*2;
			cursor += len;
			offset += len;

		}
	}
	if(isExist){
		//duplicate operation is the same, whether the leaf is full or not
		RC rc5 = insertDuplicate(fileHandle, attribute, buffer, offset,pageNum, key, keyLen, rid);
		return rc5;
	}
	if(freeSpaceOffset + keyLen + 2*RS +2 <= PAGE_SIZE){ //the leaf is not full, insert directly
			//normal insertion
			memmove((char *)buffer + offset + keyLen + RS*2, (char *)buffer + offset, freeSpaceOffset - offset);
			memcpy((char *)buffer + offset, key, keyLen);
			memcpy((char *)buffer+offset+keyLen, &rid.pageNum, RS);
			memcpy((char *)buffer+offset+keyLen+RS, &rid.slotNum, RS);

			totalEntries += 1;
			memcpy((char *)buffer+2, &totalEntries, 2);
			freeSpaceOffset += (keyLen+RS*2);
			memcpy((char *)buffer+PAGE_SIZE-2, &freeSpaceOffset, 2);

			RC rc1 = fileHandle.writePage(pageNum,buffer);
			free(buffer);
			return rc1;
	}else{
		//split is needed
		void *tempPage = malloc(2*PAGE_SIZE);
		void *rightPage = malloc(PAGE_SIZE);

		memcpy((char *)tempPage + 2*PAGE_SIZE -2, &freeSpaceOffset, 2);
		memmove(tempPage, buffer, freeSpaceOffset);

		memmove((char *)tempPage+offset+keyLen+RS*2, (char *)tempPage+offset, freeSpaceOffset-offset);

		memcpy((char *)tempPage+offset, key, keyLen);

		memcpy((char *)tempPage+offset+keyLen, &rid.pageNum, RS);

		memcpy((char *)tempPage+offset+keyLen+RS, &rid.slotNum, RS);

		freeSpaceOffset += keyLen+RS*2;

		totalEntries += 1;
		short midPoint = 6+RS;
		short entryLen = 4;

		int cursor = 6+RS;
		int entryCounter = 0;
		unsigned int curPageNum, curSlotNum;
		while(cursor <freeSpaceOffset/2){
			if(attribute.type==TypeVarChar){
				entryLen = *(int *)((char *)tempPage +midPoint)+ 4;
			}
			midPoint += entryLen;
			curPageNum = *(unsigned int *)((char *)tempPage + midPoint);
			if(curPageNum == redirected){
				curSlotNum = *(unsigned int *)((char *)tempPage + midPoint+RS);
				int subCut = getOFNum(fileHandle,curSlotNum);
				if(subCut < 0) return -1;
				entryCounter += subCut;
			}else{
				entryCounter ++;
			}
			midPoint += RS*2;
			cursor += entryLen + RS*2;
		}
//
//		for (int i=0; i<totalEntries/2; i++){
//			if(attribute.type == TypeVarChar){
//				entryLen = *(int *)((char *)tempPage +midPoint)+ 4;
//			}
//			midPoint += entryLen + RS*2;
//		}
//		short rightEntriesNum = totalEntries - (totalEntries/2);
		short rightEntriesNum = totalEntries - entryCounter;

		//set up right page's directory & entries, append it

		short rightFreeSpaceOffset = freeSpaceOffset - midPoint + 6 + RS;
		writeDirectory(rightPage, parentNum, rightEntriesNum, isLeaf,  rightSibling, rightFreeSpaceOffset);

		memmove((char *)rightPage + 6 + RS, (char *)tempPage+midPoint,freeSpaceOffset-midPoint);
		RC rc2 = fileHandle.appendPage(rightPage);

		if(rc2<0) {
			return rc2;
		}
		//find the entry to lift

		short midEntryLen  =  attribute.type==TypeVarChar ? *(int *)((char *)rightPage + 6 + RS)+4 :4 ;
		//midEntry = malloc(midEntryLen);

		memcpy(midEntry, (char *)rightPage + 6 + RS, midEntryLen);
		//update left leaf page
		//totalEntries /= 2;
		short leftEntriesNum = entryCounter;
		unsigned int newRightSibling = fileHandle.getNumberOfPages()-1;
		writeDirectory(buffer, parentNum, leftEntriesNum, isLeaf,  newRightSibling, midPoint);
		memcpy((char *)buffer + 6+RS, (char *)tempPage + 6+RS, midPoint-6-RS);
		RC rc3 = fileHandle.writePage(pageNum,buffer);
		//push the mid entry to the parent node, update the index:
		updateParent = true;
		rightChildPageNum = newRightSibling;
		free(buffer);
		free(tempPage);
		free(rightPage);
		//free(midEntry);
		return rc3;
	}
}


RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void *key,
		const RID &rid)
{

	if(! ixfileHandle.fileHandle.alreadyOpen()) {
		return -1;
	}

	unsigned int keyLen=0;
	RC rc;
	if(ixfileHandle.fileHandle.getNumberOfPages() ==0){
		//if the number of pages is 0: build a root(which is also a leaf)
		//while building the root, also insert the key

		rc = firstLeaf(ixfileHandle, attribute, key, keyLen, rid);
		//and copy to the page directly
	}else{

		stack<unsigned int>internalStack;
		rc = getLeafNode(ixfileHandle.fileHandle, attribute, key, internalStack);
		if(rc < 0) {
			return rc;
		}

		/***********need check**********/

		unsigned int leafPage = internalStack.empty() ? 1 : internalStack.top();
		if(! internalStack.empty()){
			internalStack.pop();
		}
		unsigned int rightChildPageNum = redirected;
		bool updateParent = false;
		void *keyToLift = malloc(attribute.length+4);
		rc = insertToLeaf(ixfileHandle.fileHandle,attribute,leafPage, key, rid , keyToLift, rightChildPageNum, updateParent);
		if(updateParent && rightChildPageNum != redirected){
			unsigned int parentPageNum = internalStack.empty() ? redirected : internalStack.top();
			if(!internalStack.empty()){
				internalStack.pop();
			}
			insertInternalNode(ixfileHandle.fileHandle, attribute, parentPageNum, leafPage, rightChildPageNum, keyToLift, internalStack);

		}
		free(keyToLift);
	} //else{

	return rc;
}

void getEntryFromOFP( FileHandle &fileHandle, void *overflow, const unsigned int &pageNum, const RID &rid, short &newOffset, bool &isFound){
	fileHandle.readPage(pageNum, overflow);
	newOffset = 0;
	short freeSpaceOffset = *(short *)((char *)overflow+PAGE_SIZE-2-RS);
	short cursor = 0;
	isFound = false;
	unsigned int currentPageNum, currentSlotNum;
	//search in the current page:
	for(; cursor<freeSpaceOffset; cursor +=2*RS){
		currentPageNum = *(unsigned int *)((char *)overflow + cursor);
		currentSlotNum = *(unsigned int *)((char *)overflow + cursor + RS);
		if(currentPageNum==rid.pageNum && currentSlotNum==rid.slotNum){
			newOffset = cursor;
			isFound = true;
			break;
		}
	}
}

RC deleteEntryInLeaf(FileHandle &fileHandle, const unsigned int &leafPageNum, const Attribute &attribute, const void *key, const RID &rid){
	if(!fileHandle.alreadyOpen()) return -1;
	void *buffer = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(leafPageNum,buffer);
	if(rc<0) return rc;

	bool doubled = false;
	unsigned int rightSibling;
	short isRoot, totalEntries, isLeaf, freeSpaceOffset;
	getDirectory(buffer,isRoot, totalEntries, isLeaf, rightSibling, freeSpaceOffset,doubled);
	//if(rc<0) return rc;

	//compare and find the entry to delete
	bool isExist = false;
	short offset = 6+RS;
	for(int cursor = 6+RS; cursor<freeSpaceOffset;){
		int res= compareWithKey(buffer, offset, key, attribute);
		if(res>=0){
			if(res==0) isExist=true;
			break;
		}else {
			int len = attribute.type==TypeVarChar ? *(int *)((char *)buffer + offset)+4+ 2*RS : 4 + 2*RS;
				cursor += len;
				offset += len;
		}
	}

	if(!isExist) return -1; //no entry satisfies, wrong
	//if exist, delete
	int keyLen = attribute.type==TypeVarChar ? *(int *)key + 4 :4 ;
	unsigned int entryPageNum = *(unsigned int *)((char *)buffer+offset+keyLen);
	unsigned int entrySlotNum = *(unsigned int *)((char *)buffer+offset+keyLen+RS);

	if(entryPageNum != redirected){// there is no overflow page, we only need to delete this one
		if(entryPageNum==rid.pageNum && entrySlotNum==rid.slotNum){
			short endOffset = offset+keyLen+2*RS;
			memmove((char *)buffer+offset, (char *)buffer+endOffset, freeSpaceOffset-endOffset);
			totalEntries -= 1;
			memcpy((char *)buffer+2, &totalEntries, 2);
			freeSpaceOffset -= (keyLen+2*RS);
			memcpy((char *)buffer+PAGE_SIZE-2, &freeSpaceOffset, 2);
			rc = fileHandle.writePage(leafPageNum,buffer);
			free(buffer);
			return rc;
		} else{//wrong RID
			free(buffer);
			return -1;
		}
	}else{//there is overflow page, we need to read through and delete the right rid
		void *overflow = malloc(PAGE_SIZE);
		short newOffset=0;
		bool isFound=false;
		unsigned int currentPageNum = entrySlotNum;

		getEntryFromOFP(fileHandle,overflow, currentPageNum, rid, newOffset, isFound);
		unsigned int nextPageNum = *(unsigned int *)((char *)overflow+PAGE_SIZE-RS);
		short freeSpaceOffset  = *(short *)((char *)overflow+PAGE_SIZE-2-RS);

		if(!isFound){ //not found in the first page, then search all the following pages
			while(nextPageNum != redirected){
				currentPageNum = nextPageNum;
				getEntryFromOFP(fileHandle,overflow, currentPageNum, rid, newOffset, isFound);
				if(isFound) break;
				nextPageNum = *(unsigned int*)((char *)overflow +PAGE_SIZE -RS);
			}
		}
		if(isFound){
			//delete in the current overflow page
			memmove((char *)overflow+newOffset, (char *)overflow+newOffset+2*RS, freeSpaceOffset-newOffset-2*RS);
			freeSpaceOffset -= 2*RS;
			memcpy((char *)overflow+PAGE_SIZE-4, &freeSpaceOffset, 2);
			rc = fileHandle.writePage(currentPageNum,overflow);

			totalEntries -= 1;
			memcpy((char *)buffer+2, &totalEntries, 2);
			rc = fileHandle.writePage(leafPageNum,buffer);

			free(overflow);
			free(buffer);
		}else{
			//not found the rid, wrong
			free(overflow);
			free(buffer);
			rc = -1;
		}
		return rc;
	}
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//ixfileHandle.fileHandle;

	stack<unsigned int> internalStack;
	RC rc = getLeafNode(ixfileHandle.fileHandle, attribute, key, internalStack);
	if(rc<0){
		return rc;
	}

	unsigned int leafPage = internalStack.empty() ? 1 : internalStack.top();
	if(!internalStack.empty()){
		internalStack.pop();
	}

	RC rc1 = deleteEntryInLeaf(ixfileHandle.fileHandle, leafPage, attribute, key, rid);
    return rc1;
}


string printOverflowPage(FileHandle &fileHandle, const unsigned int &pageNum){
	string output = "";
	void *overflow = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,overflow);
	short freeSpaceOffset = *(short *)((char *)overflow+PAGE_SIZE-2-RS);
	unsigned int nextPageNum = *(unsigned int *)((char *)overflow + PAGE_SIZE -RS);
	short start = 0;
	unsigned int ridPageNum, ridSlotNum;
	//first page
	for(int i=0;i<freeSpaceOffset/(2*RS); i++){
		ridPageNum = *(unsigned int *)((char *)overflow+start);
		start += RS;
		ridSlotNum = *(unsigned int *)((char *)overflow+start);
		start += RS;

		output = output + "(" + to_string(ridPageNum) + "," + to_string(ridSlotNum) +")";
		if(i<freeSpaceOffset/4-1) output += ",";
	}
	//(1,1),(1,2),(2,3)
	//following overflow pages
	while(nextPageNum != redirected){
		fileHandle.readPage(nextPageNum, overflow);
		nextPageNum = *(unsigned int *)((char *)overflow + PAGE_SIZE -RS);
		freeSpaceOffset = *(short *)((char *)overflow+PAGE_SIZE-2-RS);
		start = 0;
		for(int i=0;i<freeSpaceOffset/(2*RS); i++){
			ridPageNum = *(unsigned int *)((char *)overflow+start);
			start += RS;
			ridSlotNum = *(unsigned int*)((char *)overflow+start);
			start += RS;

			output = output + ",(" + to_string(ridPageNum) + "," + to_string(ridSlotNum) +")";
		}
	}
	return output;
}

//printLeaf(fileHandle,node,attribute,depth,totalEntries);
void printLeaf(FileHandle &fileHandle, void *leafNode, const Attribute &attribute, const int &depth, const short &totalEntries){
	if(totalEntries == 0) return;

	short freeSpaceOffset = *(short *)((char *)leafNode + PAGE_SIZE -2);
	for (int i=0; i<depth; i++){
		printf("\t");
	}
	printf("{\"Keys\":[");
	//bool isExist = false;
	short start = 6+RS;

	for (int cursor=6+RS; cursor < freeSpaceOffset;){
		if(cursor!= 6+RS) printf(",");
		printf("\"");
		switch(attribute.type){
		case TypeInt:
			printf("%d:", *(int *)((char *)leafNode+start));
			start += 4;
			cursor += 4;
			break;
		case TypeReal:
			printf("%.1f:",*(float *)((char *)leafNode+start));
			start += 4;
			cursor += 4;
			break;
		case TypeVarChar:
			int stringLen = *(int *)((char *)leafNode + start);
			for(int j=0; j<stringLen; j++){
				printf("%c ",*((char *)leafNode+start+4+j));
			}
			printf(":");
			start += (4+stringLen);
			cursor +=(4+stringLen);
			break;
		}
		printf("[");
		//check if there is overflow page
		unsigned int entryPageNum = *(unsigned int *)((char *)leafNode+start);
		unsigned int entrySlotNum = *(unsigned int *)((char *)leafNode+start+RS);
		start += RS*2;
		cursor += RS*2;
		if(entryPageNum == redirected){
			//isExist = true;
			//overflow page
			string output = printOverflowPage(fileHandle, entrySlotNum);
			cout<<output;
		}else{// only one record
			printf("(%d,", entryPageNum);
			printf("%d)", entrySlotNum);
		}
		printf("]\"");
	}
	printf("]}");
}
//printInternalNode(fileHandle, attribute, 0, rootPageNum);
void printInternalNode(FileHandle &fileHandle, const Attribute &attribute, const int &depth, const unsigned int &printPageNum){
	void *node = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(printPageNum,node);
	if(rc<0) return;

	unsigned int rightSibling;
	short isRoot, totalEntries, isLeaf, freeSpaceOffset;
	bool doubled = false;
	getDirectory(node,isRoot, totalEntries, isLeaf, rightSibling, freeSpaceOffset,doubled);

	if(isLeaf == 1){
		printLeaf(fileHandle,node,attribute,depth,totalEntries);
		free(node);
		return;
	}
	//print internal nodes
	for(int i=0; i<depth ; i++){
		printf("\t");
	}
	printf( "{\"Keys\":[");

	vector<unsigned int> childrenVector;
	//short end = *(short *)((char *)node+PAGE_SIZE-2);
	short start = 6+RS;
	unsigned int childNum = *(unsigned int *)((char *)node + start);
	childrenVector.push_back(childNum);

	start += RS;
	for (short i=0; i<totalEntries; i++){
		if(i != 0) printf(",");
		switch(attribute.type){
		case TypeInt:
			printf("%d ", *(int *)((char *)node+start));
			start += 4;
			break;
		case TypeReal:
			printf("%.1f ",*(float *)((char *)node +start));
			start += 4;
			break;
		case TypeVarChar:
			int stringLen = *(int *)((char *)node + start);
			printf("\"");
			for(int j=0; j<stringLen; j++){
				printf("%c ",*((char *)node+start+4+j));
			}
			printf("\"");
			start += 4+stringLen;
			break;
		}
		childNum = *(unsigned int *)((char *)node + start);
		childrenVector.push_back(childNum);
		start += RS;
	}
	printf("],\n");
	for(int i=0; i<depth ; i++){
		printf("\t");
	}
	printf( "{\"Children\":[\n");
	//print out children
	int numOfChildren = childrenVector.size();
	for(int i=0; i<numOfChildren; i++){
		printInternalNode(fileHandle, attribute, depth+1, childrenVector[i]);
		if(i != numOfChildren-1){
			printf(",");
		}
		printf("\n");
	}

	for(int i=0; i<depth ; i++){
		printf("\t");
	}
	printf( "]}\n");
	free(node);

}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	FileHandle fileHandle = ixfileHandle.fileHandle;
	unsigned int numOfPages = fileHandle.getNumberOfPages();
	if(numOfPages == 0) return;
	unsigned int rootPageNum = getRootPage(fileHandle);
	//When root is leaf; this situation will be solved in printInternalNode

	printInternalNode(fileHandle, attribute, 0, rootPageNum);
	return;

}

bool isValid(const Attribute &attribute, const void *lowKey, const void *highKey, const bool lowI, const bool highI) {
	if(lowKey == NULL || highKey == NULL) return true;
	int off = 0;
	if(isEmpty(lowKey, off) || isEmpty(highKey, off)) return true;
	short offset = 0;
	 int res = compareWithKey(highKey, offset, lowKey, attribute);
	 if(res < 0) return false;
	 if(res == 0 && (lowI == false || highI == false)){
		 return false;
	 }
	 return true;
}

RC findLeftPage(FileHandle &fileHandle, unsigned int &pageNum){
	unsigned int rootPage = getRootPage(fileHandle);
	void *buffer = malloc(PAGE_SIZE);
	short isLeaf;
	while(true){
		fileHandle.readPage(rootPage, buffer);
		isLeaf = *(short *)((char *)buffer + 4);
		if(isLeaf){
			break;

		}else{
			rootPage = *(unsigned int *)((char *)buffer + 6 +RS);
		}
		//rootPage = *(short *)((char *)buffer + 8);
	}
	pageNum = rootPage;
	free(buffer);
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	if(! ixfileHandle.fileHandle.alreadyOpen() || ixfileHandle.fileHandle.getNumberOfPages() == 0){
		//cout << "failed here? ix 1068" << ixfileHandle.fileHandle.getNumberOfPages() << endl;

		 return -1;
	}
	if(!isValid(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive)){
		return -1;
	}
	unsigned int curPage = redirected;
	if(lowKey == NULL){
	//	cout << "come to line 1078 ix" << endl;
		findLeftPage(ixfileHandle.fileHandle, curPage);

	}else{
		stack<unsigned int> internalStack;
		RC rc1 = getLeafNode(ixfileHandle.fileHandle, attribute, lowKey , internalStack); // find leaf;
		if(rc1 < 0 || internalStack.empty()){
			return -1;
		}
		curPage = internalStack.top();
		internalStack.pop();
	}
	void *page = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(curPage, page);
	short offset = lowKey == NULL ? 6+RS : -1;
	//void *key = malloc(54);

	void *overFlow = malloc(PAGE_SIZE);
	RID rid;
	rid.pageNum = redirected;
	rid.slotNum = redirected;
//	ix_ScanIterator.initializeSI(ixfileHandle, ixfileHandle.fileHandle, attribute, curPage,
//		        		offset, lowKeyInclusive, highKeyInclusive, key, page, lowKey, highKey, overFlow, rid);
	ix_ScanIterator.initializeSI(ixfileHandle, ixfileHandle.fileHandle, attribute, curPage, offset, lowKeyInclusive, highKeyInclusive, page, lowKey, highKey, overFlow, rid);
	return 0;

}



IX_ScanIterator::IX_ScanIterator()
{
	curPage = redirected;
	lowIncluded = false;
	highIncluded = false;
	offSet = -1;
	curLeaf = NULL;
	low = NULL;
	high = NULL;
	overFlow = NULL;
	overFlowOffset = -1;
//	inOverFlow = false;

}

IX_ScanIterator::~IX_ScanIterator()
{
}


bool validOffset(const void* page, const short &offset, const void *low, const void *high, const Attribute &attribute, const bool lowI, const bool highI){
	if(low == NULL  && high == NULL) return true;
	int comp1;
	if(low == NULL ){
		comp1 = 1;
	}else{
	//	cout << "come to line 1134" << endl;
		comp1 = compareWithKey(page, offset, low, attribute);
	}
	int comp2;
	if(high == NULL){
		comp2 = -1;
	}else{
	//	cout << "come to line 1141" << endl;
		comp2 = compareWithKey(page, offset, high, attribute);
	}
	//int comp2 = compareWithKey(page, offset, high, attribute);
	if((comp1 == 0 && lowI) || comp1 > 0){
		if((comp2 == 0 && highI) || comp2 == -1){
				return true;
		}
	}
	return false;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(offSet == -2) return IX_EOF;
	unsigned int nextPage;
	short parent, totalKeys, leaf, freeSpace;
	bool doubled = false;
	getDirectory(curLeaf, parent, totalKeys, leaf, nextPage, freeSpace, doubled);

	while(offSet == -1 || offSet >= freeSpace){
	//	cout<<"total keys"<<totalKeys<<endl;
		if(low == NULL) {
			offSet = 6+RS;
			if(offSet >= freeSpace){

				if(nextPage == redirected) return IX_EOF;
				fileHandle.readPage(nextPage,curLeaf);
				curPage = nextPage;
				getDirectory(curLeaf, parent, totalKeys, leaf, nextPage, freeSpace, doubled);
				offSet = -1;

			}
		}
		else{
			//this is for the first time scan to find the starting point.
			short start = 6+RS;
			//int cursor = 6+RS;
			while(totalKeys != 0 && start<freeSpace) {
			//	cout << "come to line 1178" << endl;
				int res = compareWithKey(curLeaf, start, low, attribute);
				if(res >= 0) {
					offSet = start;
					break;
				}
				short len = attribute.type == TypeVarChar? *(int *)((char *)curLeaf + start) + 4 : 4;
				start += len;
				start += 2*RS;
				//cursor += (len + 2*RS);
			}
		//	if(nextPage == -1) return IX_EOF;
//			if(offSet == 8 && isEmpty(curLeaf, offSet) && nextPage==-1){
//			 				return IX_EOF;
//			 	}
			if(totalKeys == 0 || start >= freeSpace){
				if( nextPage == redirected){
					return IX_EOF;
				}
				fileHandle.readPage(nextPage, curLeaf);
				curPage = nextPage;
			    getDirectory(curLeaf, parent, totalKeys, leaf, nextPage, freeSpace, doubled);
			}
		}
	}
	//here is just for overflow page detection:

		if(validOffset(curLeaf,offSet, low, high, attribute, lowIncluded, highIncluded)){
			int keyLen = attribute.type == TypeVarChar? *(int *)((char *)curLeaf + offSet) + 4 : 4;
			memcpy(key, (char *)curLeaf + offSet, keyLen);

			while(*(unsigned int *)((char *)curLeaf + offSet + keyLen) == redirected){//for this key, there is overflow page
				if(overFlowOffset == -1){ //first time read overflow
					unsigned int overflowPage = *(unsigned int *)((char *)curLeaf + offSet + keyLen + RS);
					fileHandle.readPage(overflowPage, overFlow);
					overFlowOffset = 0;
				}
				short freeSpaceOF = *(short *)((char *)overFlow + PAGE_SIZE - RS-2);
				unsigned int nextOF = *(unsigned int *)((char *)overFlow + PAGE_SIZE-RS);

				while(overFlowOffset >= freeSpaceOF && nextOF != redirected){
					fileHandle.readPage(nextOF, overFlow);
					freeSpaceOF = *(short *)((char *)overFlow + PAGE_SIZE - RS -2);
					nextOF = *(unsigned int *)((char *)overFlow + PAGE_SIZE-RS);
					overFlowOffset = 0;
				}
				if(overFlowOffset >= freeSpaceOF && nextOF == redirected){
					offSet += (keyLen + 2*RS);
					if((offSet >= freeSpace && nextPage == redirected) || ! validOffset(curLeaf,offSet, low, high, attribute, lowIncluded, highIncluded)){
						return IX_EOF;
					}// change to next page:
					if(offSet >= freeSpace){
						fileHandle.readPage(nextPage, curLeaf);
						offSet = 6+RS; //that is for the non-first page scan;
						curPage = nextPage;
						getDirectory(curLeaf, parent, totalKeys, leaf, nextPage, freeSpace, doubled);
						while((totalKeys == 0 || offSet >= freeSpace) && nextPage != redirected) {
							fileHandle.readPage(nextPage, curLeaf);
							offSet = 6+RS; //that is for the non-first page scan;
							curPage = nextPage;
							getDirectory(curLeaf, parent, totalKeys, leaf, nextPage, freeSpace, doubled);
						}
						if(nextPage == redirected && (totalKeys == 0 || offSet >= freeSpace)){
							return IX_EOF;
						}
						if(totalKeys != 0 && offSet < freeSpace && !validOffset(curLeaf,offSet, low, high, attribute, lowIncluded, highIncluded)){
							return IX_EOF;
						}
						//keyLen = attribute.type == TypeVarChar? *(int *)((char *)curLeaf + offSet) + 4 : 4;

					}
					keyLen = attribute.type == TypeVarChar? *(int *)((char *)curLeaf + offSet) + 4 : 4;
					memcpy(key, (char *)curLeaf + offSet, keyLen);
					overFlowOffset = -1;

				}
				else{
					///stil in this overflow page
					rid.pageNum = *(unsigned int *)((char *)overFlow + overFlowOffset);
					rid.slotNum = *(unsigned int *)((char *)overFlow + overFlowOffset + RS);
				    overFlowOffset += 2*RS;
				    return 0;

				}
			}


			rid.pageNum = *(unsigned int*)((char *)curLeaf + offSet + keyLen);
			rid.slotNum = *(unsigned int*)((char *)curLeaf + offSet + keyLen + RS);
			offSet += (keyLen + 2*RS);

		}else{
			return IX_EOF;
		}


	if(offSet >= freeSpace) {
		if(nextPage == redirected) {
			offSet = -2;
		}else{
			fileHandle.readPage(nextPage, curLeaf);
			offSet = 6+RS; //that is for the non-first page scan;
		   curPage = nextPage;
		}
	}

	return 0;
}

RC IX_ScanIterator::close()
{
	curPage = redirected;
	lowIncluded = false;
	highIncluded = false;
	offSet = -1;
	curLeaf = NULL;
	low = NULL;
	high = NULL;
	overFlow = NULL;
	overFlowOffset = -1;
	free(overFlow);
    free(curLeaf);
	return 0;
}


IXFileHandle::IXFileHandle()
{
	fileHandle = FileHandle();
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}
bool IXFileHandle::alreadyOpen(){
	return fileHandle.alreadyOpen();
}
RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    //how & where are the counters udpated?
	ixReadPageCounter = fileHandle.readPageCounter;
    ixWritePageCounter = fileHandle.writePageCounter;
    ixAppendPageCounter = fileHandle.appendPageCounter;

    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;

	return 0;
}

