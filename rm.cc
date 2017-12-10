
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{

}

RelationManager::~RelationManager()
{
}

int getNextID(int &nextID) {

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi ;
	FileHandle fileHandle;
	rbfm->openFile(TABLE,fileHandle);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);

	vector<string> conditionAttribute;
	conditionAttribute.push_back("table-id");
	void *value = NULL;
	RC rc=rbfm->scan(fileHandle,tableDescriptor,"",NO_OP,value,conditionAttribute,rbfmsi);
	if(rc!=0){
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		return rc;
	}
	int currentID = -1;
	int maxID = 1;
	RID rid;
	void *data = malloc(50);
	while (rbfmsi.getNextRecord(rid, data) != RBFM_EOF) {
			currentID = *(int *)((char *)data + 1);
			if (currentID>maxID){
				maxID = currentID;
			}
		}
	nextID = currentID+1;
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	free(data);
	return 0;

}
RC getNextIndexId( int &nextID){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbfm->openFile(INDEX,fileHandle);
	if(fileHandle.getNumberOfPages()==0){
		nextID=1;
		rbfm->closeFile(fileHandle);
		return 0;
	}
	vector<Attribute> indexDescriptor;
	prepareAttribute4Index(indexDescriptor);

	vector<string> conditionAttribute;
	conditionAttribute.push_back("index-id");
	RBFM_ScanIterator rbfmsi;
	RC rc = rbfm->scan(fileHandle,indexDescriptor,"",NO_OP,NULL, conditionAttribute,rbfmsi);
	if(rc<0){
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		return rc;
	}
	//int currentID = -1;
	RID rid;
	void *data = malloc(50);
	while(rbfmsi.getNextRecord(rid,data) != RBFM_EOF){
		nextID = *(int *)((char *)data +1) +1;
	}
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	free(data);
	return 0;
}
void prepareAttribute4Table(vector<Attribute> &tableDescriptor) {
	Attribute table_id, table_name, file_name;
	table_id.name = "table-id";
	table_id.type = TypeInt;
	table_id.length = sizeof(int);

	table_name.name = "table-name";
	table_name.type = TypeVarChar;
	table_name.length = 50;

	file_name.name = "file-name";
	file_name.type = TypeVarChar;
	file_name.length = 50;

	//tableDescriptor = {};
	tableDescriptor.push_back(table_id);
	tableDescriptor.push_back(table_name);
	tableDescriptor.push_back(file_name);
}

void prepareAttribute4Column(vector<Attribute> &columnDescriptor) {
	Attribute table_id, column_name, column_type, column_length, column_position;

	table_id.name = "table-id";
	table_id.type = TypeInt;
	table_id.length = sizeof(int);

	column_name.name = "column-name";
	column_name.type = TypeVarChar;
	column_name.length = 50;

	column_type.name = "column-type";
	column_type.type = TypeInt;
	column_type.length = sizeof(int);

	column_length.name = "column-length";
	column_length.type = TypeInt;
	column_length.length = sizeof(int);

	column_position.name = "column-position";
	column_position.type = TypeInt;
	column_position.length = sizeof(int);

	//columnDescriptor = {};
	columnDescriptor.push_back(table_id);
	columnDescriptor.push_back(column_name);
	columnDescriptor.push_back(column_type);
	columnDescriptor.push_back(column_length);
	columnDescriptor.push_back(column_position);

}
void prepareAttribute4Index(vector<Attribute> &indexDescriptor){
	Attribute index_id, table_id, attribute_name, index_name;
	index_id.name = "index-id";
	index_id.type = TypeInt;
	index_id.length = sizeof(int);

	table_id.name = "table-id";
	table_id.type = TypeInt;
	table_id.length = sizeof(int);

	attribute_name.name = "attribute-name";
	attribute_name.type = TypeVarChar;
	attribute_name.length = 50;

	index_name.name = "index-name";
	index_name.type = TypeVarChar;
	index_name.length = 50;

	indexDescriptor.push_back(index_id);
	indexDescriptor.push_back(table_id);
	indexDescriptor.push_back(attribute_name);
	indexDescriptor.push_back(index_name);
}

void prepareRecord4Tables(const int tid, const char *tableName, int tlen, const char *fileName,
		 int flen, const int pointerSize, void *data) {
	unsigned char *nullPointer = (unsigned char *)malloc(pointerSize);
	memset(nullPointer, 0, pointerSize);
	int start = 0;
	memcpy((char *)data + start, nullPointer, pointerSize);
	start += pointerSize;
	memcpy((char *)data + start, &tid, 4);
	start += 4;
	memcpy((char *)data + start, &tlen, 4);
	start += 4;
	memcpy((char *)data + start, tableName, tlen);
	start += tlen;
	memcpy((char *)data + start, &flen, 4);
	start += 4;
	memcpy((char *)data + start, fileName, flen);
	start += flen;
	free(nullPointer);

}

//prepareRecord4Columns(tid, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i, nullPointerSize, buffer);
void prepareRecord4Columns(const int cid, const char* columnName, const int clen, AttrType type, const int len, const int pos, const int pointerSize, void * data) {
	unsigned char* nullPointer = (unsigned char *)malloc(pointerSize);
	memset(nullPointer, 0, pointerSize);
	int start = 0;
	memcpy((char *)data + start, nullPointer, pointerSize);
	start += pointerSize;
	memcpy((char *)data + start, &cid, 4);
	start += 4;
	memcpy((char *)data + start, &clen, 4);
	start += 4;
	memcpy((char *)data + start, columnName, clen);
	start += clen;
	memcpy((char *)data + start, &type, 4);
	start += 4;
	memcpy((char *)data + start, &len, 4);
	start += 4;
	memcpy((char *)data + start, &pos, 4);
	start += 4;
	free(nullPointer);

}
void prepareRecord4Index(const int iid, const int tid, const char* attributeName, const int alen,
		const char * indexName, const int ilen, const int pointerSize, void *data){
	unsigned char* nullPointer = (unsigned char *)malloc(pointerSize);
	//cout<<"attribute name in the first is "<<attributeName<<endl;
	memset((char *)nullPointer, 0, pointerSize);
	int start = 0;
	memcpy((char *)data+start, nullPointer, pointerSize);
	start += pointerSize;
	memcpy((char *)data+start, &iid, 4);
	start += 4;
	memcpy((char *)data+start, &tid, 4);
	start += 4;
	memcpy((char *)data+start, &alen, 4);
	start += 4;
	memcpy((char *)data+start, attributeName, alen);
	start += alen;
	memcpy((char *)data+start, &ilen, 4);
	start += 4;
	memcpy((char *)data+start, indexName, ilen);
	start += ilen;
	string atteName = string((char *)data+pointerSize+12, alen);
	//cout<<"the attribute name in prepare is "<<atteName<<endl;
	free(nullPointer);
}

bool tableNameOccuppied(const string  & tableName){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi;
	FileHandle fileHandle;
	rbfm -> openFile(TABLE, fileHandle);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute = "table-name";
	void *value = malloc(54);
	int nameSize = tableName.size();
	memcpy(value,&nameSize,4);
	memcpy((char *)value+sizeof(int), tableName.c_str(), nameSize);
	vector<string> attributeNames;
	attributeNames.push_back("table-id");

	//TODO****//
	RID rid;
	void *data = malloc(100);
	rbfm->scan(fileHandle, tableDescriptor, conditionAttribute,EQ_OP,value, attributeNames,rbfmsi);
	if(rbfmsi.getNextRecord(rid, data)!= -1){
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		free(data);
		free(value);
		return true;
	}
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	free(data);
	free(value);
	return false;
}

bool indexNameOccupied(const string &indexName){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbfm->openFile(INDEX,fileHandle);

	if(fileHandle.getNumberOfPages()==0) {
		rbfm->closeFile(fileHandle);
		return false;
	}
	RBFM_ScanIterator rbfmsi;
	vector<Attribute> indexDescriptor;
	prepareAttribute4Index(indexDescriptor);
	string conditionAttribute = "index-name";

	void *value = malloc(54);
	int nameSize = indexName.size();
	//cout<<"indexName is "<<indexName<< " with size nameSize"<<nameSize<<endl;
	memcpy(value, &nameSize, 4);
	memcpy((char *)value + sizeof(int), indexName.c_str(), nameSize);
	vector<string> 	attributeNames;
	attributeNames.push_back("index-name");
	RID rid;
	void *data = malloc(100);
	rbfm->scan(fileHandle,indexDescriptor,conditionAttribute,EQ_OP,value,attributeNames,rbfmsi);

	//cout<<"the rc for indexNameOccupied is "<<rc<<endl;
	if(rbfmsi.getNextRecord(rid, data) != RBFM_EOF){
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		free(data);
		free(value);
		return true;
	}
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	free(data);
	free(value);
	return false;
}

int callInsertRecord(const string &fileName, const vector<Attribute>& descriptor, const void *data, RID &rid) {
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc1 = rbfm -> openFile(fileName, fileHandle);
	if(rc1 < 0) {
		return rc1;
	}
	RC rc2 = rbfm -> insertRecord(fileHandle, descriptor, data, rid);
	rbfm -> closeFile(fileHandle);
	return rc2;
}

int callDeleteRecord(const string &fileName, const vector<Attribute>& descriptor, RID &rid) {
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc1 = rbfm -> openFile(fileName, fileHandle);
	if(rc1 < 0) {
			return rc1;
	}
	RC rc2 = rbfm -> deleteRecord(fileHandle, descriptor, rid);

	rbfm -> closeFile(fileHandle);

	return rc2;
}
RC RelationManager::createCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc1 = rbfm->createFile(TABLE);
	RC rc2 = rbfm->createFile(COLUMN);
	RC rc3 = rbfm->createFile(INDEX);

	if(rc1 < 0 || rc2 < 0 || rc3 < 0){
		return -1;
	}

	int tid = 1;
	int cid = 2;
	int iid = 3;
	// create Tables:
	FileHandle tableHandle;
	rbfm -> openFile(TABLE, tableHandle); // open Table to insert table, column;
	void *buffer = malloc(1000);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	int pointerSize = ceil((double) tableDescriptor.size() / 8);

	prepareRecord4Tables(tid, TABLE, 6 , TABLE, 6, pointerSize, buffer);
	RID rid1;
	rbfm->insertRecord(tableHandle, tableDescriptor, buffer, rid1);
	free(buffer);

	void *buffer2 = malloc(1000);
	prepareRecord4Tables(cid, COLUMN, 7, COLUMN, 7, pointerSize, buffer2);
	RID rid2;
	rbfm -> insertRecord(tableHandle, tableDescriptor, buffer2, rid2);
	free(buffer2);

	void *buffer3 = malloc(1000);
	prepareRecord4Tables(iid, INDEX, 5, INDEX, 5, pointerSize, buffer3);
	RID rid3;
	rbfm->insertRecord(tableHandle,tableDescriptor, buffer3, rid3);
	free(buffer3);

	rbfm -> closeFile(tableHandle);


	// create Columns:
	vector<Attribute> columnDescriptor, indexDescriptor;
	prepareAttribute4Column(columnDescriptor);
	prepareAttribute4Index(indexDescriptor);

	FileHandle columnHandle;
	rbfm -> openFile(COLUMN, columnHandle);

	int nullPointerSize = ceil((double) columnDescriptor.size() / 8);

	for(unsigned int i = 1; i <= tableDescriptor.size(); i++){
		Attribute attri = tableDescriptor[i-1];
		void *buffer = malloc(1000);
		prepareRecord4Columns(tid, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i, nullPointerSize, buffer);
		RID rid;
		rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
		free(buffer);
	}

	for(unsigned int i = 1; i <= columnDescriptor.size(); i++) {
		Attribute attri2 = columnDescriptor[i-1];
		void *buffer = malloc(1000);
		prepareRecord4Columns(cid, attri2.name.c_str(), attri2.name.size(), attri2.type, attri2.length, i, nullPointerSize, buffer);
		RID rid;
		rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
		free(buffer);
	}

	for(unsigned int i = 1; i <= indexDescriptor.size(); i++) {
		Attribute attri3 = indexDescriptor[i-1];
		void *buffer = malloc(1000);
		prepareRecord4Columns(iid, attri3.name.c_str(), attri3.name.size(), attri3.type, attri3.length, i, nullPointerSize, buffer);
		RID rid;
		rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
		free(buffer);
	}
	rbfm -> closeFile(columnHandle);
	//insert a record to index

//	FileHandle indexHandle;
//	rbfm->openFile(INDEX,indexHandle);
//	void *b = malloc(100);
//	int temp = ceil((double) indexDescriptor.size() / 8);
//	prepareRecord4Index(0,0,"attributeName",13,"indexName",9,temp, b);
//	RID indexrid;
//	rbfm->insertRecord(indexHandle,indexDescriptor,buffer,indexrid);
//	cout<<"all records are inserted "<<"403"<<endl;
//	free(buffer);

	return 0;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager:: instance();
    RC rc1 = rbfm -> destroyFile(TABLE);
    RC rc2 = rbfm -> destroyFile(COLUMN);
    RC rc3 = rbfm->destroyFile(INDEX);
    if(rc1 < 0 || rc2 < 0 || rc3 <0) {
    	 return -1;
    }
    return 0;
}


/*RC RelationManager::getNextTableId(RecordBasedFileManager *rbfm, int &nextTableId){
	FileHandle fileHandle;
	rbfm->openFile(TABLE,fileHandle);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);

	return 0;
}*/
RC getTableID(const string &tableName, int &tableID){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
		RBFM_ScanIterator rbfmsi=RBFM_ScanIterator();
		FileHandle fileHandleT;
		rbfm->openFile(TABLE,fileHandleT);
		vector<Attribute> tableDescriptor;
		prepareAttribute4Table(tableDescriptor);
		string conditionAttribute1 = "table-name";
		void *value1 = malloc(54);
		int nameSize = tableName.size();
		memcpy(value1, &nameSize, 4);
		memcpy((char *)value1+4,tableName.c_str(),nameSize);
		vector<string> attributeNames1;
		attributeNames1.push_back("table-id");

		RC rc=rbfm->scan(fileHandleT,tableDescriptor,conditionAttribute1,EQ_OP,value1,attributeNames1,rbfmsi);
	//	cout<<"the rc for scan is "<<rc<<endl;
		if(rc!=0){
			free(value1);
			rbfmsi.close();
			rbfm->closeFile(fileHandleT);
			return rc;
		}

		RID trid;
		void *tableRecord = malloc(200);
		//delete the record in the TABLE
		rc = rbfmsi.getNextRecord(trid,tableRecord);
		//cout<<"the rc for getNextRecord is "<<rc<<endl;
		if(rc != 0){
			free(value1);
			free(tableRecord);
			rbfmsi.close();
			rbfm->closeFile(fileHandleT);
			return -1;
		}

		tableID = *(int *)((char *)tableRecord+1);
		free(value1);
		free(tableRecord);
		rbfm->closeFile(fileHandleT);
		rbfmsi.close();
		return 1;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName){
	string indexName = tableName+"_"+attributeName;
	if(indexNameOccupied(indexName)) return -1;
	//cout<<"pass indexNameOccupied"<<endl;
	IndexManager *ix = IndexManager::instance();
	RC rc1 = ix->createFile(indexName);
	if(rc1<0) return rc1;
	int tid = -1, iid = -1;
	rc1 = getNextIndexId(iid);
	RC rc2 = getTableID(tableName, tid);
	if(rc1<0 || rc2<0) return -1;

	//cout<<"472 "<<endl;
	//update the INDEX table
	vector<Attribute> indexDescriptor;
	prepareAttribute4Index(indexDescriptor);
	int pointerSize = ceil((double) indexDescriptor.size() / 8);

	void *buffer = malloc(1000);
	prepareRecord4Index(iid, tid, attributeName.c_str(), attributeName.size(), indexName.c_str(), indexName.size(), pointerSize,buffer);
	RID rid1;
	rc2 = callInsertRecord(INDEX, indexDescriptor, buffer, rid1);
	free(buffer);
	if(rc2 < 0) return rc2;
	//cout<<"484 update index table"<<endl;
	//build the index file
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	FileHandle fileHandle;
	IXFileHandle ixfileHandle;
	rbfm->openFile(tableName,fileHandle);
	if(fileHandle.getNumberOfPages()==0){
		rbfm->closeFile(fileHandle);
		return 0;
	}
	ix->openFile(indexName,ixfileHandle);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);

	RBFM_ScanIterator rbfmsi;
	RC rc = rbfm->scan(fileHandle,recordDescriptor, "", NO_OP, NULL,attributeNames,rbfmsi);
	//cout<<"rc in 523 is "<<rc<<endl;
	if(rc<0) return rc;
	unsigned int i = 0;
	for(; i<recordDescriptor.size(); i++){
		if(recordDescriptor[i].name == attributeName) break;
	}
	if(i==recordDescriptor.size()) return -1;
	int len = recordDescriptor[i].length + 4;

	int nullPointerSize = ceil((double)recordDescriptor.size()/8);
	RID rid;
	void *data = malloc(len);
	//void *key = malloc(len);
	while(rbfmsi.getNextRecord(rid, data) != RBFM_EOF){
		memmove(data, (char *)data+nullPointerSize, len-nullPointerSize);
		rc = ix->insertEntry(ixfileHandle,recordDescriptor[i],data,rid);
	}
	free(data);
	ix->closeFile(ixfileHandle);
	rbfm->closeFile(fileHandle);
	rbfmsi.close();
	return rc;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

	// check if tableName already exist in TABLE;
	if(tableNameOccuppied(tableName)){
		return -1;
	}
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	// check tableName file already exist:
	int rc1 = rbfm -> createFile(tableName);
	if(rc1 < 0) return -1;

	// allocate a new ID in TABLE/COLUMN for new tableName to occupy:
	int newID = -1;
	getNextID(newID);

	//insert into TABLE:
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	int pointerSize = ceil((double) tableDescriptor.size() / 8);

	void *buffer = malloc(1000);
	prepareRecord4Tables(newID, tableName.c_str(), tableName.size(), tableName.c_str(), tableName.size(), pointerSize, buffer);
	RID rid1;
	RC rc2 = callInsertRecord(TABLE, tableDescriptor, buffer, rid1);
	free(buffer);
	if(rc2 < 0) return rc2;

	//insert into COLUMN:
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	int columnPointerSize = ceil((double) columnDescriptor.size() / 8);
	for(unsigned int i = 1; i <= attrs.size(); i++) {
		Attribute attribute = attrs[i-1];
		void *buffer = malloc(1000);
		prepareRecord4Columns(newID, attribute.name.c_str(), attribute.name.size(), attribute.type, attribute.length, i, columnPointerSize, buffer);
		RID rid_i;
		RC rci = callInsertRecord(COLUMN, columnDescriptor, buffer, rid_i);
		free(buffer);
		if(rci < 0) return rci;
	}
    return 0;
}
RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	//delete from INDEX
	//scan to get the records to be deleted
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi;
	FileHandle fileHandle;
	rbfm->openFile(INDEX, fileHandle);

	vector<Attribute> indexDescriptor;
	prepareAttribute4Index(indexDescriptor);
	string indexName = tableName+"_"+attributeName;
	void *value = malloc(54);
	int len = indexName.size();
	memcpy(value, &len, 4);
	memcpy((char *)value+4, indexName.c_str(),len);
	vector<string> attributeNames;
	attributeNames.push_back("index-id");

	RC rc = rbfm->scan(fileHandle,indexDescriptor,"index-name", EQ_OP, value, attributeNames, rbfmsi);
	if(rc<0) {
		free(value);
		return -1;
	}

	RID rid;
	void *data = malloc(54);

	rc = rbfmsi.getNextRecord(rid, data);

	if(rc!=0){
		free(data);
		free(value);
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		return -1;
	}

	rbfm->deleteRecord(fileHandle,indexDescriptor,rid);
	rbfmsi.close();
	free(value);
	free(data);

	rbfm->closeFile(fileHandle);
	//destroy index file
	rc = rbfm->destroyFile(indexName);
	return rc;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if(tableName == TABLE || tableName == COLUMN) return -1;

	//get the parameters for scan ready
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi;
	FileHandle fileHandleT;
	rbfm->openFile(TABLE,fileHandleT);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute1 = "table-name";
	void *value1 = malloc(54);

	int nameSize = tableName.length();
	memcpy(value1, &nameSize, 4);
	memcpy((char *)value1 + 4, tableName.c_str(), nameSize);


	vector<string> attributeNames1;
	attributeNames1.push_back("table-id");
	attributeNames1.push_back("file-name");

	//scan TABLE and find the table,get the table-id&filename
	RC rc=rbfm->scan(fileHandleT,tableDescriptor,conditionAttribute1,EQ_OP,value1,attributeNames1,rbfmsi);
	if(rc!=0){
		free(value1);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return rc;
	}

	RID trid;
	void *tableRecord = malloc(1600);
	//delete the record in the TABLE
	rc = rbfmsi.getNextRecord(trid,tableRecord);

	if(rc!=0){
		free(value1);
		free(tableRecord);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return -1;
	}

	rbfm->deleteRecord(fileHandleT,tableDescriptor,trid);
	rbfmsi.close();
	int tableID = *(int *)((char *)tableRecord+1);
	free(value1);
	free(tableRecord);

	rbfm->closeFile(fileHandleT);

	//get the parameters ready for scan the COLUMN
	FileHandle fileHandleC;
	rbfm->openFile(COLUMN,fileHandleC);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	string conditionAttribute2 = "table-id";
	void *value2 = malloc(4);
	memcpy(value2,&tableID,4);
	vector<string> attributeNames2;
	attributeNames2.push_back("table-id");

	//scan COLUMN by table-id, get the columns of the table
	rc= rbfm->scan(fileHandleC,columnDescriptor,conditionAttribute2,EQ_OP,value2,attributeNames2,rbfmsi);
	if(rc!=0){
		free(value2);
		rbfmsi.close();
		rbfm->closeFile(fileHandleC);
		return rc;
	}
	//delete all the records in COLUMN
	RID crid;
	void *columnRecord = malloc(200);
	while(rbfmsi.getNextRecord(crid,columnRecord) != RBFM_EOF){
		rbfm->deleteRecord(fileHandleC,columnDescriptor,crid);
	}
	free(value2);
	free(columnRecord);
	rbfmsi.close();
	rbfm->closeFile(fileHandleC);

	//delete the table by deleting the file
	rc = rbfm->destroyFile(tableName);
	if(rc!=0){
		return -1;
	}
    return 0;
}

Attribute fromAttribute(void *data){
	Attribute result;
	int nameSize = *(int *)((char *)data+1);
	void *attrName= malloc(nameSize);
	memcpy(attrName,(char *)data+5,nameSize);
	string attrNameS((char *)attrName,nameSize);
	result.name = attrNameS;

	int type = *(int *)((char *)data+5+nameSize);
	if(type==0){
		result.type = TypeInt;
	}else if(type==1){
		result.type = TypeReal;
 	}else{
 		result.type = TypeVarChar;
 	}

	int attrLength = *(int *)((char *)data +5+nameSize+4);
	result.length = attrLength;

	return result;
}


RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	//This method gets the attributes (attrs) of a table called tableName by looking in the catalog tables

	//given the tableName, scan TABLE and find the table-id
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi=RBFM_ScanIterator();
	FileHandle fileHandleT;
	rbfm->openFile(TABLE,fileHandleT);
	//cout<<"the TABLE has been opened"<<endl;
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute1 = "table-name";
	void *value1 = malloc(54);
	int nameSize = tableName.size();
	memcpy(value1, &nameSize, 4);
	memcpy((char *)value1+4,tableName.c_str(),nameSize);
	vector<string> attributeNames1;
	attributeNames1.push_back("table-id");

	RC rc=rbfm->scan(fileHandleT,tableDescriptor,conditionAttribute1,EQ_OP,value1,attributeNames1,rbfmsi);
//	cout<<"the rc for scan is "<<rc<<endl;
	if(rc!=0){
		free(value1);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return rc;
	}

	RID trid;
	void *tableRecord = malloc(200);
	//delete the record in the TABLE
	rc = rbfmsi.getNextRecord(trid,tableRecord);
	//cout<<"the rc for getNextRecord is "<<rc<<endl;
	if(rc != 0){
		free(value1);
		free(tableRecord);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return -1;
	}

	int tableID = *(int *)((char *)tableRecord+1);
	free(value1);
	free(tableRecord);
	rbfm->closeFile(fileHandleT);
	rbfmsi.close();

	//given the table-id, scan the COLUMN, find all the attributes and push back

	//get the parameters ready for scan the COLUMN
	FileHandle fileHandleC;
	rbfm->openFile(COLUMN,fileHandleC);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	string conditionAttribute2 = "table-id";
	void *value2 = malloc(4);
	memcpy(value2,&tableID,4);
	vector<string> attributeNames2;
	//attributeNames2.push_back("table-id");
	attributeNames2.push_back("column-name");
	attributeNames2.push_back("column-type");
	attributeNames2.push_back("column-length");


	//scan COLUMN by table-id, get the colun-name of the table
	rc= rbfm->scan(fileHandleC,columnDescriptor,conditionAttribute2,EQ_OP,value2,attributeNames2,rbfmsi);
	if(rc!=0){
		free(value2);
		rbfmsi.close();
		rbfm->closeFile(fileHandleC);
		return rc;
	}
	//push back the column names into attris
	RID crid;
	void *columnRecord = malloc(200);
	Attribute attr;
	//columnName.type = TypeVarChar;
	//columnName.length = 50;
	string columnNameStr;
	int cnt = 0;
	while(rbfmsi.getNextRecord(crid,columnRecord) != -1){
		attr = fromAttribute(columnRecord);
		attrs.push_back(attr);
		cnt++;
	}
	free(value2);
	free(columnRecord);
	rbfmsi.close();
	rbfm->closeFile(fileHandleC);
    return 0;
}

RC insertIndex(const string &indexName, const Attribute &attri, const void *key, const RID &rid){
	IndexManager *im = IndexManager::instance();
	IXFileHandle ixfileHandle;
	RC rc = im->openFile(indexName,ixfileHandle);
	if( rc != 0) return rc;

	rc=im->insertEntry(ixfileHandle,attri,key,rid);
	if(rc<0) return rc;

	rc = im->closeFile(ixfileHandle);
	return rc;
}

RC deleteIndex(const string &indexName, const Attribute &attri, const void *key, const RID &rid){
	IndexManager *im = IndexManager::instance();
	IXFileHandle ixfileHandle;
	RC rc = im->openFile(indexName,ixfileHandle);
	if(rc<0) return rc;

	rc = im->deleteEntry(ixfileHandle,attri,key,rid);
	if(rc<0) return rc;

	rc= im->closeFile(ixfileHandle);
	return rc;
}

RC getIndexAttributes(const string &tableName, vector<string> & indexNames){
	//given the tableName, find all the attributes that have index
	//scan the INDEX file
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi = RBFM_ScanIterator();
	FileHandle fileHandle;
	rbfm->openFile(INDEX,fileHandle);
	vector<Attribute> indexDescriptor;
	prepareAttribute4Index(indexDescriptor);
	//int len = tableName.size();
	//cout<<"the length of tableName "<<len<<endl;
	int tableID=0;
	getTableID(tableName, tableID);

//	void *value = malloc(len+4);
//	memcpy(value, &len, 4);
//	memcpy((char *)value+4, tableName.c_str(), len);
	void *value = malloc(4);
	memcpy(value, &tableID, 4);
	vector<string> attributeNames;
	attributeNames.push_back("attribute-name");
	RC rc=rbfm->scan(fileHandle,indexDescriptor,"table-id",EQ_OP, value,attributeNames,rbfmsi);
	//RC rc=rbfm->scan(fileHandle,indexDescriptor,"table-id",EQ_OP,value,attributeNames,rbfmsi);
	if(rc<0) return rc;

	RID rid;
	void *data = malloc(60);
	while(rbfmsi.getNextRecord(rid,data) != RBFM_EOF){
		int l = *(int *)((char *)data+1);
		string attributeName = string((char *)data+1+4, l);
		//cout<<"the length of attribute is "<<l<<" and the attributeName is "<<attributeName<<endl;
		indexNames.push_back(attributeName);
	}
	free(data);
	free(value);
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	return 0;
}
RC getOffset(const void *data, const string &indexAttribute, const vector<Attribute> &recordDescriptor, int &offset, int &keyLen){
	int nullPointerSize = ceil((double)recordDescriptor.size()/8);
	offset = nullPointerSize;
	const char *header = (char *)data;
	for(unsigned int i=0; i<recordDescriptor.size(); i++){
		bool isNull = header[i/8] & (1<< (7-i%8));
		if(recordDescriptor[i].name != indexAttribute){
			if(isNull == 0) offset += recordDescriptor[i].type==TypeVarChar ? *(int *)((char *)data+offset)+4 : 4;
		}else{//we found the attribute
			if(isNull == 1) {//this attribute is null, return -1;
				offset = -1;
			}else{
				keyLen = recordDescriptor[i].type==TypeVarChar ? *(int *)((char *)data+offset)+4 : 4;
			}
			break;
		}
	}
	return 0;
}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if(tableName == TABLE ||tableName == COLUMN) return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc = rbfm->openFile(tableName,fileHandle);
	if(rc==-1) return rc;

	vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName,recordDescriptor);
	if(rc==-1) return rc;

	rc = rbfm->insertRecord(fileHandle,recordDescriptor,data,rid);
	if(rc==-1){
		rbfm->closeFile(fileHandle);
		return rc;
	}

	//insert to index
	vector<string> indexNames;

	getIndexAttributes(tableName, indexNames);
//cout<<"indexNames length"<<indexNames.size()<<"indexName is " <<endl;
	if(indexNames.size()==0){//no index file
		rbfm->closeFile(fileHandle);
		return 0;
	}
	//there is some index files
	void *key = malloc(54);
	for(string s:indexNames){
		string indexName = tableName + "_" + s;
		unsigned int i=0;
		for(; i<recordDescriptor.size(); i++){
			if(recordDescriptor[i].name == s) break;
		}
		if(i==recordDescriptor.size()) return -1;

		//get the key right
		int offset = 0, keyLen =0;
		getOffset(data, s, recordDescriptor,offset,keyLen);
		if(offset ==-1) key = NULL;
		else{
			memcpy(key, (char *)data + offset, keyLen);
		}
		insertIndex(indexName, recordDescriptor[i], key, rid);
	}
	free(key);
	rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if(tableName == TABLE ||tableName == COLUMN) return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc = rbfm->openFile(tableName,fileHandle);
	if(rc==-1) return rc;

	vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName,recordDescriptor);
	if(rc==-1) return rc;
	//void *data  = malloc(PAGE_SIZE);
	/*****************TO DELETE INDEX*********************/
	void *data = malloc(PAGE_SIZE);
	readTuple (tableName,rid, data);
	/*****************TO DELETE INDEX*********************/
	rc = rbfm->deleteRecord(fileHandle,recordDescriptor,rid);
	if(rc==-1){
		rbfm->closeFile(fileHandle);
		return rc;
	}
	//delete the index if needed

	vector<string> indexNames;
	getIndexAttributes(tableName, indexNames);

	if(indexNames.size()==0){//no index file
		rbfm->closeFile(fileHandle);
		return 0;
	}
	//there is some index files
	void *key = malloc(54);
	for(string s:indexNames){
		string indexName = tableName + "_" + s;
		unsigned int i=0;
		for(; i<recordDescriptor.size(); i++){
			if(recordDescriptor[i].name == s) break;
		}
		if(i==recordDescriptor.size()) return -1;

		//get the key right
		int offset = 0, keyLen =0;
		getOffset(data, s, recordDescriptor,offset,keyLen);
		if(offset ==-1) key = NULL;
		else{
			memcpy(key, (char *)data + offset, keyLen);
		}
		deleteIndex(indexName, recordDescriptor[i], key, rid);
	}
	free(key);
	rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(tableName == TABLE ||tableName == COLUMN) return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc = rbfm->openFile(tableName,fileHandle);
	if(rc==-1) return rc;

	vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName,recordDescriptor);
	if(rc==-1) return rc;

/*****************TO DELETE INDEX*********************/
	void *oldData = malloc(PAGE_SIZE);
	readTuple (tableName,rid, oldData);
/*****************TO DELETE INDEX*********************/

	rc = rbfm->updateRecord(fileHandle,recordDescriptor,data,rid);
	if(rc==-1){
		rbfm->closeFile(fileHandle);
		return rc;
	}
//delete all the old entries
	vector<string> indexNames;
	getIndexAttributes(tableName, indexNames);

	if(indexNames.size()==0){//no index file
		rbfm->closeFile(fileHandle);
		return 0;
	}
	//there is some index files
	void *key = malloc(54);
	void *key2 = malloc(54);
	for(string s:indexNames){
		string indexName = tableName + "_" + s;
		unsigned int i=0;
		for(; i<recordDescriptor.size(); i++){
			if(recordDescriptor[i].name == s) break;
		}
		if(i==recordDescriptor.size()) return -1;

			//get the key right
		int offset = 0, keyLen =0;
		getOffset(oldData, s, recordDescriptor,offset,keyLen);
		if(offset ==-1) key = NULL;
		else{
			memcpy(key, (char *)oldData + offset, keyLen);
		}
		deleteIndex(indexName, recordDescriptor[i], key, rid);

		int offset2 =0;
		int keyLen2 =0;
		getOffset(data, s, recordDescriptor,offset2,keyLen2);
		if(offset2 == -1) key2 = NULL;
		else{
			memcpy(key2, (char *)data + offset2, keyLen2);
		}
		insertIndex(indexName, recordDescriptor[i], key2, rid);
	}
	free(key);
	free(key2);

	rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rfbm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> attributes;

	RC rc1 = getAttributes(tableName, attributes);
	if(rc1 < 0){
		return -1;
	}

	RC rc2 = rfbm -> openFile(tableName, fileHandle);

	if(rc2 < 0) {
		rfbm -> closeFile(fileHandle);
		return -1;
	}

	RC rc3 = rfbm -> readRecord(fileHandle, attributes, rid, data);
	if(rc3 < 0) {
		rfbm -> closeFile(fileHandle);
		return -1;
	}
	rfbm -> closeFile(fileHandle);
	return 0;

}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc = rbfm -> printRecord(attrs, data);
	if(rc < 0){
		return -1;
	}
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	vector<Attribute> attributes;
	RC rc1 = getAttributes(tableName, attributes);
	if(rc1 < 0) {
		return -1;
	}

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc2 = rbfm -> openFile(tableName, fileHandle);
	if(rc2 < 0) {
		rbfm -> closeFile(fileHandle);
		return -1;
	}

	RC rc3 = rbfm -> readAttribute(fileHandle, attributes, rid, attributeName, data);

	if(rc3 < 0){
		rbfm -> closeFile(fileHandle);
		return -1;
	}

	rbfm -> closeFile(fileHandle);
    return 0;

}


RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{

	RBFM_ScanIterator rbfmsi = RBFM_ScanIterator();

	vector<Attribute> recordDescriptor;
	RC rc = getAttributes(tableName,recordDescriptor);
	if(rc < 0 ) return rc;

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbfm->openFile(tableName,fileHandle);
	rc = rbfm->scan(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rbfmsi);
	if(rc != 0){
		//rbfmsi.close();
		rbfm->closeFile(fileHandle);
		return -1;
	}
	rm_ScanIterator.setRBFMSI(rbfmsi);
    return 0;
}
RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

	RC rc = rbfmsi.getNextRecord(rid, data);
	if(rc== RBFM_EOF){
		return RM_EOF;
	}
	return 0;
}


// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


// Assisting functions listed below:

RC getAttribute(const string &tableName, const string &attributeName, Attribute &attri){
	int tableID = -1;
	RC rc = getTableID(tableName, tableID);
	if(rc<0) return rc;
	//scan the COLUMN table using tableID and get the Attribute attri
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi = RBFM_ScanIterator();
	FileHandle fileHandle;
	rbfm->openFile(COLUMN,fileHandle);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	void *data = malloc(4);
	memcpy(data, &tableID, 4);
	vector<string> attributeNames;
	attributeNames.push_back("column-name");
	attributeNames.push_back("column-type");
	attributeNames.push_back("column-length");

	rc = rbfm->scan(fileHandle,columnDescriptor,"table-id",EQ_OP,data, attributeNames,rbfmsi);
	RID rid;
	void *value = malloc(1+54+4+4);
	int flag =-1, nameLen=-1;
	while(rbfmsi.getNextRecord(rid,value) != RBFM_EOF){
		//check if it is the attribute we want
		nameLen = *(int *)((char *)value+1);
		string name;
		for (int i = 0; i < nameLen; i++){
			name.push_back(*(char *)((char *)value+1+4+i));
		}
		flag = strcmp(name.c_str(),attributeName.c_str());
		if(flag == 0) break;
	}
	if(flag!=0) return -1;

	//get the Attribute and return
	attri.name = attributeName;
	int type =   *(int *)((char *)value +1+4+nameLen);
	if(type==0) {
		attri.type = TypeInt;
	}else if(type == 1){
		attri.type = TypeReal;
	}else{
		attri.type = TypeVarChar;
	}
	attri.length = *(int *)((char *)value+1+4+nameLen+4);

	free(data);
	free(value);
	rbfm->closeFile(fileHandle);
	return 0;
}
 //indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	//IndexManager *ix = IndexManager::instance();
	IXFileHandle ixfileHandle;
	//IX_ScanIterator ixsi = IX_ScanIterator();
	//IX_ScanIterator ixsi = rm_IndexScanIterator.ixsi;
	string indexName = tableName+"_"+attributeName;
	Attribute attri;
	//to do:get attribute from the name
	getAttribute(tableName, attributeName, attri);
	rm_IndexScanIterator.ix->openFile(indexName,ixfileHandle);
	RC rc = rm_IndexScanIterator.ix->scan(ixfileHandle,attri,lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator.ixsi);
	if(rc<0){
		rm_IndexScanIterator.ix->closeFile(ixfileHandle);
		return rc;
	}

	//rm_IndexScanIterator.setIXSI(ixsi);
	return rc;
}
//RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {};
RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	RC rc = this ->ixsi.getNextEntry(rid,key);
	if(rc == RM_EOF){
		return RM_EOF;
	}
	return 0;

}






