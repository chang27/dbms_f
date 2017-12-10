#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <unordered_map>
#include <map>
#include <cfloat>
//#include "../rbf/rbfm.h"
#include "../rm/rm.h"
//#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;
        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
       unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if(alias) this->tableName = alias;
    };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {

                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();

        };
};

class Filter : public Iterator {
    // Filter operator
    public:


        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        ){
        		this ->it = input;
        		this -> condi = condition;
        		input -> getAttributes(attributes);

        }
        ~Filter(){};

        RC getNextTuple(void *data);

        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        	   attrs = this ->attributes;
        };
    private:
  	  Iterator * it;
  	  Condition  condi;
  	  vector<Attribute> attributes;

};


class Project : public Iterator {
    // Projection operator
    public:
		Iterator * it;
		vector<Attribute> projected;
		vector<Attribute> attrs;

        Project(Iterator *input,                    // Iterator of input R
            const vector<string> &attrNames){
        		this -> it = input;
        		input ->getAttributes(attrs);
        		for(string name : attrNames){
        			for(unsigned int i = 0; i < attrs.size(); i++){
        				if(name == attrs[i].name){
        					projected.push_back(attrs[i]);
        					break;
        				}
        			}
        		}
        };   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        			attrs = this ->projected;
        };
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        ){
        		this->leftIt = leftIn;
        		this->rightIt = rightIn;
        		this->condi = condition;
        		this->numPages = numPages;
        		this->leftData = malloc(numPages * PAGE_SIZE);
        		this -> rightPage = malloc(PAGE_SIZE);
        		this -> pos = 0;
        		//this->rightData = malloc(PAGE_SIZE);
        		leftIn -> getAttributes(leftAttrs);
        		rightIn -> getAttributes(rightAttrs);
        		for(unsigned int i = 0; i < leftAttrs.size(); i++){
        			attrs.push_back(leftAttrs[i]);
        			if(leftAttrs[i].name== condition.lhsAttr){
        				left = i;
        			}
        		}
        		for(unsigned int i = 0; i < rightAttrs.size(); i++){
        			attrs.push_back(rightAttrs[i]);
        			if(rightAttrs[i].name== condition.rhsAttr){
        			        	right = i;
        			}
        		}
        		createHashTable();
        };
        ~BNLJoin(){
        		free(leftData);
        		free(rightPage);
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        		attrs = this -> attrs;
        };
    private:
        Iterator * leftIt;
        TableScan * rightIt;
        int left = -1;
        int right = -1;
        Condition condi;
        unsigned numPages;
        vector<Attribute> leftAttrs;
        vector<Attribute> rightAttrs;
        vector<Attribute> attrs;
        void * leftData;
        unordered_map <string, vector<int> > map;
        int pos;
        void * rightPage;
        vector<int> curList;
        RC createHashTable();

};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
        	this->leftIt = leftIn;
        	this->rightIt = rightIn;
        	this->condi = condition;
        	initialize = true;
        	leftIn -> getAttributes(leftAttrs);
        	rightIn -> getAttributes(rightAttrs);

        	     for(unsigned int i = 0; i < leftAttrs.size(); i++){
        	        	attrs.push_back(leftAttrs[i]);
        	        	if(leftAttrs[i].name== condition.lhsAttr){
        	        		 left = i;
        	        		}
        	        	}
        	        	for(unsigned int i = 0; i < rightAttrs.size(); i++){
        	        		attrs.push_back(rightAttrs[i]);
        	        		if(rightAttrs[i].name== condition.rhsAttr){
        	        			   	right = i;
        	        		}
        	        	}
        	        	leftData = malloc(PAGE_SIZE);
        	        	rightData = malloc(PAGE_SIZE);
        };
        ~INLJoin(){
        		free(leftData);
        		free(rightData);

        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
        		attrs = this->attrs;
        };
    private:
        Iterator *leftIt;
        IndexScan *rightIt;

        Condition condi;
        bool initialize;
        vector<Attribute> leftAttrs;
        vector<Attribute> rightAttrs;
        vector<Attribute> attrs;
        void *leftData;
        void *rightData;
        int left;
        int right;



};

// Optional for everyone. 10 extra-credit points
//class GHJoin : public Iterator {
//    // Grace hash join operator
//    public:
//      GHJoin(Iterator *leftIn,               // Iterator of input R
//            Iterator *rightIn,               // Iterator of input S
//            const Condition &condition,      // Join condition (CompOp is always EQ)
//            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
//      ){};
//      ~GHJoin(){};
//
//      RC getNextTuple(void *data){return QE_EOF;};
//      // For attribute in vector<Attribute>, name it as rel.attr
//      void getAttributes(vector<Attribute> &attrs) const{};
//};


class GHJoin : public Iterator {
    // Grace hash join operator
public:
    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    );
    ~GHJoin();
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const{
    	   for(int i = 0; i < leftAttrs.size(); i++){
               attrs.push_back(leftAttrs[i]);
               
    	     	}
        for(int i = 0; i < rightAttrs.size(); i++){
            attrs.push_back(rightAttrs[i]);
            
        }
    };
private:
    Iterator *_leftIt;
    Iterator *_rightIt;
    Condition _condition;
    unsigned _numPartitions;
    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;
    vector<string> leftFiles;
    vector<string> rightFiles;
    AttrType leftType;
    AttrType rightType;
    unordered_map <int, vector<void*> > map;
    int leftIdx;
    //int rightIdx;
    vector<void *> leftBuf;
    void * rightBuf;
    RecordBasedFileManager *rbfm;
    RBFM_ScanIterator *rbfmsi;
    RC buildUpFiles();
    RC buildUpMaps();
    RC buildPartitions();
    RC loadRight();
    RC getPool(void *rightAttr, int len);
    RC deleteFiles();
};

struct Collector {
	float min;
	float max;
	float sum;
	float avg;
	int cnt;
	Collector(): min(FLT_MAX), max(FLT_MIN), sum(0), avg(0), cnt(0){
};
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){
        		this->it = input;
        		this->aggAttr = aggAttr;
        		this->aop = op;
        	//	this -> sumValue = 0;
        	//	this -> minValue = FLT_MAX;
        	//	this -> count = 0;
        	//	this -> average = 0;
        		this -> isGroupBy = false;
        		this ->toEnd = false;
        		//this ->maxValue = FLT_MIN;
        		this ->c.max = FLT_MIN;
        		this -> c.sum = 0;
        		this -> c.min = FLT_MAX;
        		this -> c.cnt = 0;
        		this -> c.avg = 0;
        		this -> preparedGB = false;
        };

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        ){
        		this -> it = input;
        		this -> aggAttr = aggAttr;
        		this -> groupAttr = groupAttr;
        		this -> aop = op;
        		this ->isGroupBy = true;
        		this ->toEnd = false;
        		this ->c.max = FLT_MIN;
        		this -> c.sum = 0;
        		this -> c.min = FLT_MAX;
        		this -> c.cnt = 0;
        		this -> c.avg = 0;
        		this -> preparedGB = false;

        };
        ~Aggregate(){
        		this -> it = NULL;
        };

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator* it;
        Attribute aggAttr;
        Attribute groupAttr;
        AggregateOp aop;
        Collector c;
        bool isGroupBy;
        bool toEnd;
        map<string, Collector> group_map;
        bool preparedGB;
        RC updateAggregate(AttrType type, const void* value, Collector & c);
        RC getNextTupleGroup();

};

#endif
