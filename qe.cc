
#include "qe.h"

int getConditionAttr(const vector<Attribute> & fullattributes, const void *data, void *output, string & condition){
//get the value of the condition attribute
	int fieldSize = fullattributes.size();
	int pointerSize = ceil((double) fullattributes.size() / 8);
	int start = pointerSize;

	for(int i = 1; i <= fieldSize; i++){
		Attribute attribute = fullattributes[i-1];
		bool null = *((unsigned char *)data + (i - 1)/8) & (1 << (8 - 1 - (i-1)%8));
// not the selection attribute

		if(attribute.name != condition){
			if(!null){
				int varCharLen = *(int *)((char *)data+start);

				start += attribute.type == TypeVarChar ? (varCharLen+ 4) : 4;
			}
			continue;
		}else{

		if(null){
				output = NULL;
				return 0;
		}else{
			if(attribute.type == TypeVarChar) {
				int varLen = *(int *)((char *)data + start);
				memcpy(output, (char *)data + start, 4 + varLen);
				return 4 + varLen;
			}else{
				memcpy(output, (char *)data + start, 4);
				return 4;
			}
		}
		}

	}
	return 0;
}

int getPosition(const vector<Attribute> &attrs, string toMatch){
	unsigned int pos = 0;
	for(; pos < attrs.size(); pos++){
		if(attrs[pos].name == toMatch){
			break;
		}
	}
	return pos == attrs.size() ? -1 : pos;
}
bool passComparison(const Value rightValue, int rlen, CompOp op, void *leftValue, int llen, AttrType ltype){
	if(rlen == 0 && llen == 0) return true;
	if(rightValue.type != ltype) return false;
	//cout << " p 53" << endl;
	if(ltype == TypeInt){
		switch(op){
			case EQ_OP:
				return (*(int *)rightValue.data == *(int *)leftValue);
			case LT_OP:
				return (*(int *)leftValue < *(int *)rightValue.data);
			case LE_OP:
				return (*(int *)leftValue <= *(int *)rightValue.data);
			case GT_OP:
				return (*(int *)leftValue > *(int *)rightValue.data);
			case GE_OP:
				return (*(int *)leftValue >= *(int *)rightValue.data);
			case NE_OP:
				return (*(int *)rightValue.data != *(int *)leftValue);
			case NO_OP:
				return true;
		}
	}else if( ltype == TypeReal){
		switch(op){
				case EQ_OP:
					return (*(float *)rightValue.data == *(float *)leftValue);
				case LT_OP:
					return (*(float *)leftValue < *(float *)rightValue.data);
				case LE_OP:
					return (*(float *)leftValue <= *(float *)rightValue.data);
				case GT_OP:
					return (*(float *)leftValue > *(float *)rightValue.data);
				case GE_OP:
					return (*(float *)leftValue >= *(float *)rightValue.data);
				case NE_OP:
					return (*(float *)rightValue.data != *(float *)leftValue);
				case NO_OP:
					return true;
				}
	}else if(ltype == TypeVarChar) {
				string left;
				//int len = min(llen, rlen);
				for (int i = 0; i < llen -4; i++){
					left.push_back(*(char *)((char *)leftValue+4+i));
				}
				string right;
				for (int i = 0; i < rlen - 4; i++){
					right.push_back(*(char *)((char *)rightValue.data+4+i));
				}
			//	cout << left << "  " << right << endl;
		switch(op){
		        case EQ_OP:
		        		return (strcmp(left.c_str(), right.c_str()) == 0);
		        		break;
		        case LT_OP:
		        		return (strcmp(left.c_str(), right.c_str()) < 0);
		        		break;
		        case LE_OP:
		        		return (strcmp(left.c_str(), right.c_str()) <= 0);
		        		break;
		        case GT_OP:
		        		return (strcmp(left.c_str(), right.c_str()) > 0);
		        		break;
		        case GE_OP:
		        		return (strcmp(left.c_str(), right.c_str()) >= 0);
		        		break;
		        case NE_OP:
		        		return (strcmp(left.c_str(), right.c_str()) != 0);
		        		break;
		        case NO_OP:
		        		return true;
		}
	}
	return false;
}
RC Filter::getNextTuple(void *data) {
	if(it->getNextTuple(data) == -1){
		return QE_EOF;
	}
	//new data got from iterator:
	int l = getPosition(attributes, condi.lhsAttr);
	if(l == -1) return -1;

	void *left = malloc(attributes[l].length + 4);
	int leftLen = getConditionAttr(attributes, data, left, condi.lhsAttr);
	AttrType leftType = attributes[l].type;

	Value rightValue;
	int rightLen = 0;
	//to check right side:
	if(condi.bRhsIsAttr){

		int r = getPosition(attributes, condi.rhsAttr);
		rightValue.type = attributes[r].type;;
		rightValue.data = malloc(attributes[r].length + 4);
		rightLen = getConditionAttr(attributes, data, rightValue.data, condi.rhsAttr);

	}else{
		rightValue.data = condi.rhsValue.data;
		rightValue.type = condi.rhsValue.type;
		rightLen = condi.rhsValue.type == TypeVarChar? *(int *)((char *)rightValue.data) + 4 : 4;
	}
	while(! passComparison(rightValue, rightLen, condi.op, left, leftLen, leftType)){
	//	cout << leftLen << endl;
		if(it -> getNextTuple(data) == -1){
			return QE_EOF;
		}
		leftLen = getConditionAttr(attributes, data, left, condi.lhsAttr);
		if(condi.bRhsIsAttr){
			rightLen = getConditionAttr(attributes, data, rightValue.data, condi.rhsAttr);
		}
	}
	free(left);
	free(condi.bRhsIsAttr? rightValue.data: NULL);
	return 0;
}


RC Project::getNextTuple(void *data) {
	void * fullRecord = malloc(PAGE_SIZE);
	RC rc = it -> getNextTuple(fullRecord);
	if(rc < 0) {
		free(fullRecord);
		return -1;
	}

	int pointerSize = ceil( (double) attrs.size() / 8);

	int projectedSize = ceil((double) projected.size() / 8);

	int pSize = projected.size();
	// to generate nullpointer format for the projected record;
	int nullArray[pSize];


	int poffset = 0;
	for(int i = 0; i < pSize; i++){
		Attribute attribute = projected[i];
		int offset = pointerSize;
		for(unsigned int j = 1; j <= attrs.size(); j++){
			bool null = *((unsigned char *)fullRecord + (j - 1)/8) & (1 << (8 - 1 - (j-1)%8));
			if(attribute.name != attrs[j-1].name){
				if(!null){
					int len = attrs[j-1].type == TypeVarChar?  *(int *)((char *)fullRecord + offset) : 0;
					offset += len + 4;
				}
				continue;
			}else{

			if(null){
				nullArray[i] = 1;
				break;
			}else{
				nullArray[i] = 0;
				int len = attrs[j-1].type == TypeVarChar?  *(int *)((char *)fullRecord + offset) : 0;
				memcpy((char *)data + projectedSize + poffset, (char *)fullRecord + offset, len + 4);
				offset += len+4;
				poffset += len + 4;
				break;
			}
			}
		}
	}
	int SHIFT = 8;
	unsigned char *nullPointer = (unsigned char *) malloc(projectedSize);
	for(int i = 0; i < projectedSize; i++) {
	    int sum = 0;
	    int j = SHIFT*i;
	    while(j < (i+1)*SHIFT && j < pSize){
	        sum |= nullArray[j]*(1 << (SHIFT -1 - j%SHIFT));
	        j++;
	    }
	    *(nullPointer + i) = sum;
	}
	memcpy((char *)data, nullPointer, projectedSize);
	free(nullPointer);
	free(fullRecord);
	return 0;
}
//getRecordSize()

string extractKey(const void * data, const int pos, const short fieldSize, const AttrType type){
	short begin = pos == 0? 0 : *(short *)((char *)data  + 2*pos);
	short end = *(short *)((char *)data + 2*(1 + pos));
	if(begin == end) return NULL;

	if(type == TypeVarChar){
		return string((char *)data + begin + 2*(fieldSize + 1) + 4, *(int *)((char *)data + begin + 2*(fieldSize + 1)));

	}else if(type == TypeInt){
		return to_string(*(int *)((char *)data + begin + 2*(fieldSize + 1)));
	}else if(type == TypeReal){
		return to_string(*(float *)((char *)data + begin + 2*(fieldSize + 1)));
	}
	return NULL;
}

string getKey(const AttrType type, void * value){
	if(type == TypeVarChar){
			return string((char *)value+4, *(int *)value);

		}else if(type == TypeInt){
			return to_string(*(int *)value);
		}else if(type == TypeReal){
			return to_string(*(float *)value);
		}
		return NULL;
}

RC BNLJoin ::createHashTable(){//build a hash table of the left
	map.clear();
	bool created = false;
	void * buffer = malloc(PAGE_SIZE);
	int fieldSize = leftAttrs.size();
	int sizeofheader = (fieldSize + 1)*2;
	void *header = malloc(sizeofheader);
	void *tmp = malloc(PAGE_SIZE);
	int pointerSize = ceil((double) leftAttrs.size() / 8);
	int start = 0;
	while(leftIt ->getNextTuple(buffer) == 0){
		int size = formatHeader(buffer, header, leftAttrs, fieldSize, pointerSize);
		int nullsize = pointerSize + size - sizeofheader;
		if((unsigned)(nullsize + start) > PAGE_SIZE * numPages) break;
		memcpy((char *)leftData + start, buffer, nullsize);

	    memcpy((char *)tmp, header, 2*fieldSize + 2);
	    memcpy((char *)tmp + 2*fieldSize + 2, (char *)buffer + pointerSize, size - sizeofheader);
	    string key = extractKey(tmp, left, fieldSize, leftAttrs[left].type);
	    if(map.find(key) == map.end()){
	    		//vector<int> *newVec = new
	    		map[key] = vector<int>();;


	    }
	  //  cout << start << endl;
	    	map[key].push_back(start);
	    start += nullsize;
	    created = true;
	}
	free(buffer);
	free(header);
	rightIt -> setIterator();
	return created? 0 : -1;
}
int combineHeader(int leftpointerSize, int rightpointerSize, int leftFieldSize, int rightFieldSize, void * header, const void * leftRecord, const void * rightRecord) {
		int array[leftFieldSize + rightFieldSize];
		//leftNullHeader+rightNullHeader
		for(int i = 0; i < leftFieldSize + rightFieldSize;){
			for(int j = 1; j <= leftFieldSize; j++){
				bool null = *((unsigned char *)leftRecord + (j - 1)/8) & (1 << (8 - 1 - (j-1)%8));
				array[i] = null? 1 : 0;
				i++;
			}
			for(int k = 1; k <= rightFieldSize; k++){
				bool null = *((unsigned char *)rightRecord + (k - 1)/8) & (1 << (8 - 1 - (k-1)%8));
				array[i] = null? 1 : 0;
				i++;
			}
		}
		int pointerSize = ceil((double) (leftFieldSize + rightFieldSize) / 8);
        unsigned char *nullPointer = (unsigned char *) malloc(pointerSize);
        short SHIFT = 8;
        for(int i = 0; i < pointerSize; i++) {
            int sum = 0;
            int j = SHIFT*i;
            while(j < (i+1)*SHIFT && j < leftFieldSize + rightFieldSize){
                sum |= array[j]*(1 << (SHIFT -1 - j%SHIFT));
                j++;
            }
            *(nullPointer + i) = sum;
        }
        memcpy((char *)header, nullPointer, pointerSize);
        return pointerSize;
}
RC combineRecords(void *buffer, vector<Attribute> & leftAttrs, vector<Attribute> & rightAttrs,
		const void* leftRecord, const void*rightRecord){

	int leftpointerSize =  ceil((double) leftAttrs.size() / 8);
	int rightpointerSize = ceil((double) rightAttrs.size() / 8);

	int leftFieldSize = leftAttrs.size();
	int rightFieldSize = rightAttrs.size();

	void *leftHeader = malloc(1000);
	void *rightHeader = malloc(1000);

	int leftsize = formatHeader(leftRecord, leftHeader, leftAttrs, leftFieldSize, leftpointerSize) - 2 * (leftFieldSize + 1);
	int rightsize = formatHeader(rightRecord, rightHeader, rightAttrs, rightFieldSize, rightpointerSize) - 2 * (rightFieldSize + 1);

	void *header = malloc(leftpointerSize + rightpointerSize);
	int headerLen = combineHeader(leftpointerSize, rightpointerSize, leftFieldSize, rightFieldSize, header, leftRecord, rightRecord);
	memcpy(buffer, header, headerLen);
	memcpy((char *)buffer + headerLen, (char *)leftRecord + leftpointerSize, leftsize);
	memcpy((char *)buffer + headerLen + leftsize, (char *)rightRecord + rightpointerSize, rightsize);
	free(leftHeader);
	free(rightHeader);
	free(header);
	return 0;
}

RC BNLJoin::getNextTuple(void *data) {
	bool jumpout = false;
	RC rightRC;
//	void *rightPage = malloc(PAGE_SIZE);
	//need to read in new record from right:
	while(pos == 0){
		if(!jumpout){
			rightRC = rightIt -> getNextTuple(rightPage);
		}
		if(rightRC == -1){
			RC leftRC = createHashTable();
			//rightIt -> setIterator();
			if(leftRC == QE_EOF) {
				//	free(rightPage);
				return -1;
			}

			rightRC = rightIt -> getNextTuple(rightPage);
			if(rightRC == -1) return -1; // no record on the right side;
		}
		void *attribute = malloc(rightAttrs[right].length + 4);
		while(rightRC == 0){

			int len = getConditionAttr(rightAttrs, rightPage, attribute, rightAttrs[right].name);
			if(len != 0){
				string rightKey = getKey(rightAttrs[right].type, attribute);
				//cout << "print key" << rightKey << endl;
				if(map.find(rightKey) != map.end() && map[rightKey].size() != 0){
					//cout << "get matched record" << endl;
					curList = map[rightKey];
					int start = curList[pos];
					void *leftRecord = (char *)leftData + start;
					combineRecords(data, leftAttrs, rightAttrs, leftRecord, rightPage);
					//combineTwoRecord();
					//check if pos is the last element
					if((unsigned)pos == curList.size() - 1){
						curList.clear();
						pos = 0;
					}else{
						pos++;
					}
					leftRecord = NULL;
					free(attribute);
					return 0;
				}else{
					rightRC = rightIt -> getNextTuple(rightPage);
				}
			}else{
				rightRC = rightIt -> getNextTuple(rightPage);

			}
		} //while rightRC
		if(!jumpout) {
			jumpout = true;
		}

	} //while pos
	if(pos > 0 && curList.size() > 0){
		int start = curList[pos];
		void *leftRecord = (char *)leftData + start;
		combineRecords(data, leftAttrs, rightAttrs, leftRecord, rightPage);

		if((unsigned)pos == curList.size() - 1){
				curList.clear();
				pos = 0;
		}else{
				pos++;
		}
		leftRecord = NULL;
		return 0;
	}
	//check if leftpage has no records:
	return -1;
}

RC INLJoin:: getNextTuple(void *data){
	void *buffer = malloc(PAGE_SIZE);
	RC rcl = 0;
	RC rcr = 0;
	if(!initialize){
		rcr = rightIt -> getNextTuple(rightData);
		if(rcr < 0){
			initialize = true;
		}

	}
	// at the beginning or reach the end of right index:
	if(initialize){
		//leftData = malloc(PAGE_SIZE);
		rcl = leftIt -> getNextTuple(leftData);
		if(rcl < 0){
		//	free(leftData);
			free(buffer);
			return QE_EOF;
		}

		int len = getConditionAttr(leftAttrs, leftData, buffer, condi.lhsAttr); ;
		while(buffer == NULL || len == 0){
			rcl = leftIt -> getNextTuple(leftData);
			if(rcl < 0) {
				free(buffer);
			//	free(leftData);
				return QE_EOF;
			}
			len = getConditionAttr(leftAttrs, leftData, buffer, condi.lhsAttr);
		}
		rightIt -> setIterator(buffer, buffer, true, true);

		rcr = rightIt -> getNextTuple(rightData);
		initialize = false;
	}
	while(rcl == 0){
		if(rcr == 0){
			combineRecords(data, leftAttrs, rightAttrs, leftData, rightData);
			free(buffer);
			return 0;
		}else{
			rcl = leftIt -> getNextTuple(leftData);
					if(rcl < 0){
						free(buffer);
						return QE_EOF;
					}
					void *buffer = malloc(PAGE_SIZE);
					int len = getConditionAttr(leftAttrs, leftData, buffer, condi.lhsAttr); ;
					while(len == 0){
						rcl = leftIt -> getNextTuple(leftData);
						if(rcl < 0) {
							free(buffer);
						//	free(leftData);
							return QE_EOF;
						}
						len = getConditionAttr(leftAttrs, leftData, buffer, condi.lhsAttr);
					}
					rightIt -> setIterator(buffer, buffer, true, true);
					//free(buffer);
					rcr = rightIt -> getNextTuple(rightData);

		}

	}

}

void Aggregate ::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	if(isGroupBy){
		attrs.push_back(groupAttr);
	}
		Attribute attri;
		if(aop == MIN){
			attri.name = "MIN(" + aggAttr.name + ")";
			attri.type = aggAttr.type;
			attri.length = aggAttr.length;
		}else if(aop == MAX){
			attri.name = "MAX(" + aggAttr.name + ")";
			attri.type = aggAttr.type;
			attri.length = aggAttr.length;
		}else if(aop == SUM) {
			attri.name = "SUM("  + aggAttr.name + ")";
			attri.type = aggAttr.type;
			attri.length = aggAttr.length;
		}else if(aop == COUNT) {
			attri.name = "COUNT(" + aggAttr.name + ")";
			attri.type = aggAttr.type;
			attri.length = aggAttr.length;
		}else if(aop == AVG){
			attri.name = "AVG(" + aggAttr.name + ")";
			attri.type = aggAttr.type;
			attri.length = aggAttr.length;
		}

		attrs.push_back(attri);

}

RC Aggregate:: updateAggregate(AttrType type, const void* value, Collector & c) {
	//float temp = *(float *)res;
	switch(aop){
	case MIN:
		if(type == TypeInt){
			int val;
			memcpy(&val, (char *)value, 4);
			c.min = min(c.min, (float) val);

		}else if(type == TypeReal){
			float val;
			memcpy(&val, (char *)value, 4);
			c.min = min(c.min, val);

		}else{
			return -1;
		}
	//	memcpy(res, &minValue, sizeof(float));
		break;
	case MAX:
		if(type == TypeInt){
				int val;
				memcpy(&val, (char *)value, 4);
				c.max = max(c.max, (float) val);
		}else if(type == TypeReal){
				float val;
				memcpy(&val, (char *)value, 4);
				c.max = max(c.max, val);
		}else {
			return -1;
		}
	//	memcpy(res, &maxValue, sizeof(float));
		break;
	case SUM:
		if(type == TypeInt){
			int val;
			memcpy(&val, (char *)value, 4);
			c.sum += (float) val;
		}else if(type == TypeReal){
			float val;
			memcpy(&val, (char *)value, 4);
			c.sum += val;
		}else{
			return -1;
		}
	//	memcpy(res, &sumValue, sizeof(float));
		break;
	case COUNT:
		c.cnt++;
	//	memcpy(res, &count, sizeof(float));
		break;
	case AVG:
		c.cnt++;
		if(type == TypeInt){
			int val;
			memcpy(&val, (char *)value, 4);
			c.sum += (float) val;
		}else if(type == TypeReal){
			float val;
			memcpy(&val, (char *)value, 4);
			c.sum += val;
		}else{
			return -1;
		}
		c.avg = c.sum/c.cnt;
	//	memcpy(res, &average, sizeof(float));
		break;

	}
	return 0;
}
void generateData(const Collector c, const string key, const AttrType type, void *data, AggregateOp op){
	memset((char *)data, 0, 1);
	int len = 4;
	if(type == TypeInt){
		int groupKey = stoi(key.c_str());
		memcpy((char *)data + 1, &groupKey, len);
	}else if(type == TypeReal){
		float groupKey = stof(key.c_str());
		memcpy((char *)data + 1, &groupKey, len);
	}else if(type == TypeVarChar){
		len = key.size();
		memcpy((char *)data + 1, &len, 4);
		memcpy((char *)data + 1 + 4, key.c_str(), len);
		len += 4;
	}
	switch(op){
					case MIN:
						memcpy((char *)data + 1 + len, &c.min, 4);
						break;
					case MAX:
						memcpy((char *)data + 1 + len, &c.max, 4);
						break;
					case COUNT:
						memcpy((char *)data + 1 + len, &c.cnt, 4);
						break;
					case AVG:
						memcpy((char *)data + 1 + len, &c.avg, 4);
						break;
					case SUM:
						memcpy((char *)data + 1 + len, &c.sum, 4);
					}

}
RC Aggregate::getNextTuple(void *data){
	if(toEnd || aggAttr.type == TypeVarChar){
		return -1;
	}
	if(!isGroupBy){
		void *data1 = malloc(PAGE_SIZE);
			vector<Attribute> attrs;
			it -> getAttributes(attrs);
			unsigned int i = 0;
			for(; i < attrs.size(); i++){
				if(attrs[i].name == aggAttr.name){
					break;
				}
			}
			// if the aggregate attribute not exists:
			if(i == attrs.size()) return -1;
			bool existvalue = false;
			//void* res = malloc(4);
			while(it -> getNextTuple(data1) == 0){
				void *attribute = malloc(aggAttr.length + 4);
				if(getConditionAttr(attrs, data1, attribute, aggAttr.name) != 0){
					if(!existvalue) existvalue = true;
					updateAggregate(aggAttr.type, attribute, c);
				}
				free(attribute);
			}
			if(!existvalue){
				unsigned char nul = 0x80;
				memcpy((char *)data, &nul, 1);
			}else{
				float f = (float) c.cnt;
				memset((char *)data, 0, 1);
			switch(aop){
				case MIN:
					memcpy((char *)data + 1, &c.min, 4);
					break;
				case MAX:
					memcpy((char *)data + 1, &c.max, 4);
					break;
				case COUNT:
					memcpy((char *)data + 1, &f, 4);
					break;
				case AVG:
					memcpy((char *)data + 1, &c.avg, 4);
					break;
				case SUM:
					memcpy((char *)data + 1, &c.sum, 4);
				}
			}
			toEnd = true;
			return 0;
	}else{

		if(!preparedGB){
			RC rc = getNextTupleGroup();
			preparedGB = true;
			if(rc < 0) return QE_EOF;
		}
		if(group_map.empty()) return QE_EOF;
		map<string, Collector>::iterator iterator = group_map.begin();
		while(iterator != group_map.end()){
			string key = iterator -> first;
			Collector c = iterator -> second;
			generateData(c, key, groupAttr.type, data, aop);
			group_map.erase(iterator);
			return 0;
		}

	}
	return QE_EOF;
}

RC Aggregate::getNextTupleGroup(){

	vector<Attribute> attrs;
	it->getAttributes(attrs);
	unsigned int i = 0;
	bool flagA = false;
	bool flagG = false;
	for(; i < attrs.size(); i++){
		 if(attrs[i].name == aggAttr.name){
						flagA = true;
			}
		 if(attrs[i].name == groupAttr.name){
			 flagG = true;
		 }
	}
	if(! flagA || !flagG){
		return -1;
	}
	void *buffer = malloc(PAGE_SIZE);
	while(it -> getNextTuple(buffer) == 0){
		void *attr1 = malloc(aggAttr.length + 4);
		void *attr2 = malloc(groupAttr.length + 4);
		int len1 = getConditionAttr(attrs, buffer, attr1, aggAttr.name);
		int len2 = getConditionAttr(attrs, buffer, attr2, groupAttr.name);
		if(len2 == 0 || len1 == 0) {
			free(attr1);
			free(attr2);
			continue;
		}
		string key =  getKey(groupAttr.type, attr2);
		if(group_map.find(key) == group_map.end()){
				group_map[key] = Collector();
		}
		updateAggregate(aggAttr.type, attr1, group_map[key]);
		free(attr1);
		free(attr2);
	}
	return group_map.empty()? -1 : 0;
}
//Modified Bernstein hash function
unsigned int firstHashFunction(const void *input, AttrType type){
    if(type == TypeInt){
        return (unsigned int) *(int *)input;
    }else if(type == TypeReal){
        float f = *(float *)input;
        unsigned int h = (unsigned int) f*33 + 1;
        //cout << h << endl;
        return (unsigned int) f*33 + 1;
    }else if(type == TypeVarChar){
        int len = *(int *)input;
        unsigned int h = 0;
        char *p = (char *)input + 4;
        for(int i = 0; i < len; i++){
            h = 33 * h + p[i];
        }
        return h;
    }
    return -1;
}
// use one at a time hash function:
unsigned int secondHashFunction(const void *input, AttrType type){
    unsigned int h = 0;
    if(type == TypeInt){
        h += ~(*(int *)input << 10);
        h ^= (h >> 6);
        h += ~(h << 3);
        h ^= (h >> 11);
        h += ~(h << 15);
        return h;
    }
    else if(type == TypeReal){
        float f = *(float *)input;
        h += ~((unsigned int)f << 10);
        h ^= (h >> 6);
        h += ~(h << 3);
        h ^= (h >> 11);
        h += ~(h << 15);
        //    cout << h << endl;
        return h;
    }else if(type == TypeVarChar){
        
        int len = *(int *)input;
        char *p = (char *)input + 4;
        for (int i = 0; i < len; i++)
        {
            h += p[i];
            h += (h << 10);
            h ^= (h >> 6);
        }
        h += (h << 3);
        h ^= (h >> 11);
        h += (h << 15);
        
        return h;
    }
    return -1;
}

//Iterator *_leftIt;
//Iterator *_rightIt;
//Condition _condition;
//unsigned _numPartitions;
//vector<Attribute> leftAttrs;
//vector<Attribute> rightAttrs;
//vector<string> leftFiles;
//vector<string> rightFiles;
//unordered_map <string, vector<void*> > map;
//int leftIdx;
//int rightIdx;
//vector<void *> leftBuf;
//void * rightBuf;
//RecordBasedFileManager *rbfm;
//RBFM_ScanIterator *rbfmsi;
GHJoin :: GHJoin(Iterator *leftIn,               // Iterator of input R
                 Iterator *rightIn,               // Iterator of input S
                 const Condition &condition,      // Join condition (CompOp is always EQ)
                 const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
){
    this -> _leftIt = leftIn;
    this -> _rightIt = rightIn;
    this -> _condition = condition;
    this -> _numPartitions = numPartitions;
    this ->leftIdx = -1;
    // this -> rightIdx = 0;
    leftIn -> getAttributes(leftAttrs);
    for(int i = 0; i < leftAttrs.size(); i++){
        if(leftAttrs[i].name == _condition.lhsAttr){
            leftType = leftAttrs[i].type;
            break;
        }
    }
    
    rightIn -> getAttributes(rightAttrs);
    for(int i = 0; i < rightAttrs.size(); i++){
        if(rightAttrs[i].name == _condition.rhsAttr){
            rightType = rightAttrs[i].type;
            break;
        }
    }
    this -> rightBuf = malloc(PAGE_SIZE);
    rbfm = RecordBasedFileManager::instance();
    RC rc1 =  buildUpFiles();
    RC rc2 = buildPartitions();
 //   cout << "buildPartitions() " << rc2 << endl;
    RC rc = buildUpMaps();
    
    if(rc < 0) return;
    this -> rbfmsi = 0;
    rightBuf = malloc(PAGE_SIZE);
    loadRight();
};
GHJoin::~GHJoin(){
    free(rightBuf);
    if(map.size() != 0){
        
        unordered_map<int, vector<void*> >::iterator itr = map.begin();
        while(itr != map.end())
        {
            vector<void *> &v = itr->second;
            for(int j = 0; j < v.size(); j++){
                free(v[j] == NULL? v[j] : NULL);
            }
            map.erase(itr->first);
            itr++;
        }
        
    }
    map.clear();
    if(rbfmsi != 0){
        rbfmsi -> close();
        rbfmsi = 0;
    }
    while(! leftBuf.empty()){
        auto i = leftBuf.begin();
        leftBuf.erase(i);
    }
    deleteFiles();
    
    
};
string getName(int idx, string name, int t){
    
    if(t == 0){
        return "left_join_"  + name + "_" + to_string(idx) ;
    }else {
        return "right_join_" + name + "_" + to_string(idx) ;
    }
}
RC GHJoin::buildUpFiles(){
    //FileHandle fileHandle;
    RC rc;
    for(int i = 0; i < _numPartitions; i++){
        //FileHandle fileHandle;
        string filenameL = getName(i, _condition.lhsAttr, 0);
        rc = rbfm -> createFile(filenameL);
        if(rc < 0) return -1;
        leftFiles.push_back(filenameL);
        string filenameR = getName(i, _condition.rhsAttr, 1);
        rc = rbfm -> createFile(filenameR);
        if(rc < 0) return -1;
        rightFiles.push_back(filenameR);
    }
    return 0;
}

RC GHJoin::buildPartitions(){
    void *buffer = malloc(PAGE_SIZE);
    bool exist = false;
    while(_leftIt->getNextTuple(buffer) != QE_EOF)
    {
        void *attribute = malloc(PAGE_SIZE);
        int len = getConditionAttr(leftAttrs, buffer, attribute, _condition.lhsAttr);
        if(len == 0) continue;
        unsigned int key = firstHashFunction(attribute, leftType);
        unsigned int idx = key % _numPartitions;
        //cout << idx << "left idx" <<endl;
        RID rid;
        FileHandle fileHandle;
        rbfm -> openFile(leftFiles[idx].c_str(), fileHandle);
        rbfm -> insertRecord(fileHandle, leftAttrs, buffer, rid);
        rbfm -> closeFile(fileHandle);
        free(attribute);
        memset(buffer, 0, PAGE_SIZE);
        exist = true;
    }
    if(!exist) {
        free(buffer);
        return -1;
    }
    while(_rightIt->getNextTuple(buffer) != QE_EOF){
        void *attribute = malloc(PAGE_SIZE);
        int len = getConditionAttr(rightAttrs, buffer, attribute, _condition.rhsAttr);
        if(len == 0) continue;
        unsigned int key = firstHashFunction(attribute, rightType);
        unsigned int idx = key % _numPartitions;
        RID rid;
        FileHandle fileHandle1;
        rbfm -> openFile(rightFiles[idx].c_str(), fileHandle1);
        rbfm -> insertRecord(fileHandle1, rightAttrs, buffer, rid);
        rbfm -> closeFile(fileHandle1);
        free(attribute);
        memset(buffer, 0, PAGE_SIZE);
        
    }
    free(buffer);
    return 0;
}

RC GHJoin::buildUpMaps(){
   // cout << "buildUpMaps"<< endl;
    if(map.size() != 0){
        
        unordered_map<int, vector<void*> >::iterator itr = map.begin();
        while(itr != map.end())
        {
            vector<void *> &v = itr->second;
            for(int j = 0; j < v.size(); j++){
                free(v[j] == NULL? v[j] : NULL);
            }
            map.erase(itr++);
           // itr++;
        }
        map.clear();
    }
    
    //cout << "go through 976" << endl;
    
    RBFM_ScanIterator *si = new RBFM_ScanIterator();
    FileHandle fileHandle;
    bool exist = false;
    void *buffer = malloc(PAGE_SIZE);
    
    if(leftIdx == _numPartitions - 1) {
        free(buffer);
        return -1;
    }
    string fileName = leftFiles[++leftIdx]; // open a fileName, if it is empty, need to continue to next file;
    //cout <<leftIdx << endl;
    rbfm->openFile(fileName, fileHandle);
    vector<string> leftAttrNames;
    for(int i = 0; i < leftAttrs.size(); i++){
        leftAttrNames.push_back(leftAttrs[i].name);
    }
    RC rc = rbfm->scan(fileHandle, leftAttrs, "", NO_OP, NULL, leftAttrNames, *si);
    if(rc < 0){
        return -1;
    }
    RID rid;
    
    while(si -> getNextRecord(rid, buffer) != RBFM_EOF){
        void *attribute = malloc(PAGE_SIZE);
        int len = getConditionAttr(leftAttrs, buffer, attribute, _condition.lhsAttr);
        if(len == 0) continue;
        unsigned int key = secondHashFunction(attribute, leftType);
        unsigned int idx = key % _numPartitions;
        void *value = malloc(PAGE_SIZE);
        memcpy(value, buffer, PAGE_SIZE);
        if(!map.count(idx)){
            map[idx] = vector<void *>();;
        }
        map[idx].push_back(value);
        exist = true;
        free(attribute);
        memset(buffer, 0, PAGE_SIZE);
    }
    delete si;
    free(buffer);
    //cout << "go through 1018" << endl;
    return exist == true ? 0 : -1;
}

RC GHJoin::loadRight(){
  //  cout << "load right 1021" << endl;
    if(rbfmsi != 0){
        rbfmsi -> close();
        delete rbfmsi;
        rbfmsi = 0;
    }//reset pointer;
    if(leftIdx == _numPartitions){
        return -1;
    }
    this -> rbfmsi = new RBFM_ScanIterator();
    string fileName = rightFiles[leftIdx];
    FileHandle fileHandle;
    RC rc  = rbfm -> openFile(fileName, fileHandle);
    vector<string> rightAttrNames;
    for(int i = 0; i < rightAttrs.size(); i++){
        rightAttrNames.push_back(rightAttrs[i].name);
    }
    rc = rbfm -> scan(fileHandle, rightAttrs, "", NO_OP, NULL, rightAttrNames, *rbfmsi);
    if(rc < 0) return -1;
    return 0;
}


// getPool ganrantee to find a non-empty leftBuf:
RC GHJoin:: getPool(void *rightAttr, int len){
    while(! leftBuf.empty()){
        auto i = leftBuf.begin();
        free(*i);
        leftBuf.erase(i);
    }
    //cout << 1054 << endl;
    unsigned int key = secondHashFunction(rightAttr, rightType);
    unsigned int idx = key % _numPartitions;
    if(!map.count(idx) || map[idx].size() == 0) return -1;
    
    vector<void *>candidate = map[idx];
    void *attribute = malloc(PAGE_SIZE);
    for(int i =0; i < candidate.size(); i++){
        
        memset(attribute, 0, PAGE_SIZE);
        int lenL = getConditionAttr(leftAttrs, candidate[i], attribute, _condition.lhsAttr);
        if(lenL == 0 || len != lenL) {
            //free(attribute);
            continue;
        }
        
        if(memcmp(attribute, rightAttr, lenL) == 0){
            void *tuple = malloc(PAGE_SIZE);
            memcpy(tuple, candidate[i], PAGE_SIZE);
            leftBuf.push_back(tuple);
        }
        //free(attribute);
        
    }
    free(attribute);
    return leftBuf.empty() ? -1 : 0;
}

RC GHJoin :: getNextTuple(void *data){
    if(!leftBuf.empty()){
        void *leftRecord = leftBuf.front();
        combineRecords(data, leftAttrs, rightAttrs, leftRecord, rightBuf);
        free(leftRecord);
        leftBuf.erase(leftBuf.begin());
        return 0;
    }
    
    while(leftBuf.empty()){
        RID rid;
        //void *rightTuple = malloc(PAGE_SIZE);
        void *attribute = malloc(PAGE_SIZE);
        while(rbfmsi->getNextRecord(rid, rightBuf) == 0){
            
            int len = getConditionAttr(rightAttrs, rightBuf, attribute, _condition.rhsAttr);
            if(len == 0) continue;
            if(getPool(attribute, len) == 0 ){
                void *leftRecord = leftBuf.front();
                combineRecords(data, leftAttrs, rightAttrs, leftRecord, rightBuf);
                
                leftBuf.erase(leftBuf.begin());
                free(leftRecord);
                free(attribute);
                return 0;
            }
            
        }
        free(attribute);
        //free(attribute == NULL ? NULL : attribute);
        if(leftBuf.empty()){
            if(leftIdx == _numPartitions){
                return -1;
            }
            RC rc1 = buildUpMaps();
            
           // cout << leftIdx << endl;
            RC rc2 = loadRight();
           // cout << "after : " << leftIdx << endl;
            while(rc1 < 0 || rc2 < 0){
                if(leftIdx == _numPartitions - 1){
                    return -1;
                }
                
                rc1 = buildUpMaps();
               // cout << leftIdx << endl;
                rc2 = loadRight();
             //   cout << "after : " << leftIdx << endl;
            }
            
            
            
        }
    }
    return -1;
}

RC GHJoin::deleteFiles(){
    
    for(int i = 0; i < leftFiles.size(); i++){
        rbfm ->destroyFile(leftFiles[i]);
    }
    for(int i = 0; i < rightFiles.size(); i++){
        rbfm -> destroyFile(rightFiles[i]);
    }
    return 0;
}
