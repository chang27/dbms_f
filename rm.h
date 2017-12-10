
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"
using namespace std;

# define RM_EOF (-1)  // end of a scan operator

#define TABLE "Tables"
#define COLUMN "Columns"
#define INDEX "Index"
#define TLEN 6;
#define CLEN 7;

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close() {
	  rbfmsi.close();
	  return 0;

  };

  void setRBFMSI(RBFM_ScanIterator si){
	  rbfmsi=si;
  }

private:
  RBFM_ScanIterator rbfmsi;
};

void prepareAttribute4Table(vector<Attribute> &tableDescriptor);
void prepareAttribute4Column(vector<Attribute >&columnDescriptor);
void prepareAttribute4Index(vector<Attribute> &indexDescriptor);
  //RC appendTable(FileHandle &fileHandle, const int tid, int pointerSize, void* data);
void prepareRecord4Tables(const int tid, const char *tableName, const int tlen, const char *fileName,
  		const int flen, const int pointerSize, void *data);
void prepareRecord4Columns(const int cid, const char* columnName, const int clen,
		  AttrType type, const int len, const int pos, const int pointerSize, void * data);
void prepareRecord4Index(const int iid, const int tid, const char* tableName, const int tlen,
		const char * indexName, const int ilen, const int pointerSize, void *data);
bool tableNameOccuppied(const string  & tableName);
  RC getNextID();
  int callInsertRecord(const string &fileName, const vector<Attribute>& descriptor, const void *data, RID &rid);
  int callDeleteRecord(const string &fileName, const vector<Attribute>& descriptor, RID &rid);
  int delete4Table(const vector<Attribute> &attrs, const void*data, int &tableID, const string &attribute);
// Relation Manager


  class RM_IndexScanIterator {
    public:
 	  IX_ScanIterator ixsi;
 	  IndexManager *ix;
     RM_IndexScanIterator() {
    	 	 	 ix = IndexManager :: instance();
     		ixsi= IX_ScanIterator();
     };  	// Constructor
     ~RM_IndexScanIterator() {}; 	// Destructor

     // "key" follows the same format as in IndexManager::insertEntry()
     RC getNextEntry(RID &rid, void *key);  	// Get next matching entry

     RC close() {
     	ixsi.close();
     	return 0;};    // Terminate index scan

     void setIXSI(IX_ScanIterator si){
     	  this->ixsi=si;
       }

    //private:
     //IX_ScanIterator ixsi;
   };
  class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

     // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                           const string &attributeName,
                           const void *lowKey,
                           const void *highKey,
                           bool lowKeyInclusive,
                           bool highKeyInclusive,
                           RM_IndexScanIterator &rm_IndexScanIterator);
// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

private:

};

#endif
