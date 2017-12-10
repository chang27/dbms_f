#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define RS 4
# define redirected 0xFFFFFFFF

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
        RC initializeSI(IXFileHandle &ixfileHandle, FileHandle &fileHandle, const Attribute &attribute, const unsigned int & curPage,
        		const short &offSet,const bool lowI, const bool highI, void *curLeaf, const void *lowK,const void *highK, void *overFlow,  RID &rid){
        			//this->ixfileHandle = ixfileHandle;
        			this->fileHandle = fileHandle;
        			this->attribute = attribute;
        			this->curPage = curPage;
        			this->offSet = offSet;
        			this->lowIncluded = lowI;
        			this->highIncluded = highI;
        			this->curLeaf = curLeaf;

        			this->low = lowK == NULL ? NULL : lowK;
        			this->high = highK == NULL ? NULL : highK;
        			//bool isNUll = lowK != NULL;
        			//cout << "check input is null" << isNUll << endl;
        			//cout << "check low is null" <<(low == NULL) << endl;
        			this->rid.pageNum = rid.pageNum;
        			this->rid.slotNum = rid.slotNum;
        			this->overFlow = overFlow;
        			return 0;
        }

    private:
        //IXFileHandle ixfileHandle;
        FileHandle fileHandle;
        Attribute attribute;
        unsigned int curPage;
        short offSet;
        bool lowIncluded;
        bool highIncluded;
        void *curLeaf;
        const void *low;
        const void *high;
        void *overFlow;
        short overFlowOffset;
  //      bool inOverFlow;
        RID rid;

};



class IXFileHandle {
    public:
	FileHandle fileHandle;
	bool alreadyOpen();
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
