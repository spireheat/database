#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <limits.h>
#include "hash.cpp"
using namespace std;

#define PAGE_SIZE 4096
#define MAX_TUPLE_COUNT 197
#define TUPLE_OFFSET 800

int cmp_keyword(string name);
char* int_to_char(int n);
int char_to_int(char p[4]);
int file_size2(char* filename);
int string_copy(char *dst,int i1,int j1,char *src,int i2,int j2);

char a[4];

void select3(string col_name, vector <string> agg_op,vector <string> agg_col, 
	string name, string where_col, string op, string cons,string group_col){

	int i,j;
	if(cmp_keyword(name) || cmp_keyword(col_name) || cmp_keyword(group_col) || cmp_keyword(where_col)){
		cout << "Syntax error" << endl;
	    return;
	}
	for(i=0;i<agg_col.size();++i){
	    if(cmp_keyword(agg_col[i])){
	        cout << "Syntax error" << endl;
	        return;
	    }
	}

    char path[50];
    sprintf(path,"db/%s.tbl",name.c_str());
    FILE * fp;
    fp=fopen(path,"r");
    if(fp==NULL){   //don't have existed before
        printf("Table %s doesn’t exist\n",name.c_str());
        return;
    }
    int fsize = file_size2(path);

    //get head information
    int sum,fix,var,last_var_col_index;
    fscanf(fp,"%d,%d,%d,%d",&sum,&fix,&var,&last_var_col_index);

    vector <string> c_name;
    vector <int> c_type;
    vector <int> c_offset;
    int start_addr;
    for(i=0;i<sum;i++){
        char c_name_buffer[128]={0};
        int type;
        int offset;

        start_addr = 46 + i*150;
        fseek(fp, start_addr, SEEK_SET);
        fscanf(fp,"%s",c_name_buffer);
        c_name.push_back(c_name_buffer);

        start_addr = 46 + i*150 + 128;
        fseek(fp, start_addr, SEEK_SET);
        fscanf(fp,"%d,%d",&type,&offset);
        c_type.push_back(type);
        c_offset.push_back(offset);
    }

    //if the agg_col exists
    vector <int> agg_col_index;
    for(j=0;j<agg_col.size();j++){
    	agg_col_index.push_back(-1);
    	for(i=0;i<sum;i++){
    		if(c_name[i] == agg_col[j])
    			agg_col_index[j] = i;
    	}
    	if(agg_col_index[j] == -1 && agg_col[j] != "*"){
    		printf("Column %s doesn’t exist\n",agg_col[j].c_str());
    		return;
    	}
    }
    //agg_col is not int
	for(j=0;j<agg_col_index.size();j++){
		if(agg_col[j] == "*" && (agg_op[j] == "sum" || agg_op[j] == "avg" 
				|| agg_op[j] == "min" || agg_op[j] == "max")){
			printf("Column %s is not int and can’t be used in aggregation\n", agg_col[j].c_str());
			return;
		}
		if(c_type[agg_col_index[j]] == 1 && (agg_op[j] == "sum" || agg_op[j] == "avg" 
			|| agg_op[j] == "min" || agg_op[j] == "max")){
			printf("Column %s is not int and can’t be used in aggregation\n", agg_col[j].c_str());
			return;
		}
	}
    //if sel_col exists
    int col_index = -1;
    for(i=0;i<sum;i++){
    	if(c_name[i] == col_name)
    		col_index = i;
    }
    if(col_index == -1){
		printf("Column %s doesn’t exist\n",col_name.c_str());
		return;
	}
	//if group_col exists
    int group_col_index = -1;
    for(i=0;i<sum;i++){
    	if(c_name[i] == group_col)
    		group_col_index = i;
    }
    if(group_col_index == -1){
		printf("Column %s doesn’t exist\n",group_col.c_str());
		return;
	}
    //column and op mismatch error
	int where_col_index = -1; 
 
    int no_where = 0;
    if(where_col.length()==0 && op.length()==0 && cons.length()==0){
    	no_where = 1;
    }
    else{
    	//if where_col exists
	    for(i=0;i<sum;i++){
	    	if(c_name[i] == where_col)
	    		where_col_index = i;
	    }
	    if(where_col_index == -1){
	    	printf("Column %s doesn’t exist\n",where_col.c_str());
	    	return;
	    }

	    if(c_type[where_col_index] == 0){ //int type
	    	if(op=="like" || op=="not" || cons[0] == '\''){
	    		printf("Predicate %s %s %s error\n",where_col.c_str(),op.c_str(),cons.c_str());
	    		return;
	    	}
	    }
	    else{
	     	if(op=="<" || op=="<=" || op==">" || op==">=" || cons[0] != '\''){
	    		printf("Predicate %s %s %s error\n",where_col.c_str(),op.c_str(),cons.c_str());
	    		return;
	    	}   	
	    }
	}

	//Non-group-by column in select list
	if(col_name != group_col){
		printf("Non-group-by column %s in select list\n",col_name.c_str() );
		return;
	}
	int no_group = 0;
	if(group_col == "")
		no_group = 1;
    //print out the first line
	if(no_group == 0)
    	cout << col_name;
    if(agg_col.size()>0 && no_group == 0 )
		cout << '|';
    for(j = 0; j < agg_op.size(); j++){
    	printf("%s(%s)",agg_op[j].c_str(),agg_col[j].c_str());
    	if( j < agg_op.size() -1)
    		cout << "|";
    	else
    		cout << endl;
    }


    //select the tuple by the op and hash the tuple
    char int2char_buffer[4] = {0};
    char data_buffer[4096] = {0};
    int k,flag = 0;

    vector <int> SUM,COUNT,AVG,MIN,MAX;
	for(j = 0; j < agg_op.size(); ++j){
		SUM.push_back(0);
		COUNT.push_back(0);
		AVG.push_back(0);
		MIN.push_back(INT_MAX);  
		MAX.push_back(INT_MIN);
	}

    for(i = 1; i < (fsize/PAGE_SIZE); i++){
    	int page_start_addr = i * PAGE_SIZE;
    	fseek(fp, page_start_addr, SEEK_SET);
    	fread(data_buffer, sizeof(char),sizeof(data_buffer),fp);
    
    	string_copy(int2char_buffer,0,4,data_buffer,0,4);
    	int full = char_to_int(int2char_buffer);
      	string_copy(int2char_buffer,0,4,data_buffer,4,8);
    	int tuple_count = char_to_int(int2char_buffer);
    	//cout << "tuple_count: " << tuple_count <<endl;
    	for(j = 0; j < tuple_count; j++){
    		int tuple_offset;
    		string_copy(int2char_buffer,0,4,data_buffer,(2+j)*4,(3+j)*4);
    		tuple_offset = char_to_int(int2char_buffer);
	    	
	    	//cout << "where_col_index: " <<where_col_index<<endl;

    		int tuple_len;
    		string_copy(int2char_buffer,0,4,data_buffer,tuple_offset,tuple_offset+4);
    		tuple_len = char_to_int(int2char_buffer);
    		//cout << "tuple_len" << tuple_len << endl;
    		flag = 0;
    		if(no_where == 1){
    			flag = 1;
    		}

    		else{
	    		if(c_type[where_col_index] == 0){//int
	    			int where_col_value;
	    			int where_col_offset = tuple_offset + c_offset[where_col_index];
	    			string_copy(int2char_buffer,0,4,data_buffer, where_col_offset,where_col_offset+4);
	    			where_col_value = char_to_int(int2char_buffer);

	    			char cons_value_buffer[200] = {0};
	    			cons.copy(cons_value_buffer, cons.length(), 0);
	    			int const_value = atoi(cons_value_buffer);
	    			if(	(op == "<" && where_col_value < const_value)
	    				|| (op == "<=" && where_col_value <= const_value)
	    				|| (op == ">" && where_col_value > const_value)
	    				|| (op == ">=" && where_col_value >= const_value)
	    				|| (op == "=" && where_col_value == const_value)
	    				|| (op == "!=" && where_col_value != const_value) ){
	    					flag = 1;
	    			}
	    		}
	    		else{//varchar

					char begin_offset_buffer[4] = {0};
					char end_offset_buffer[4] = {0};

					if(where_col_index != last_var_col_index) {

						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer, tuple_offset + c_offset[where_col_index], tuple_offset + c_offset[where_col_index] + 4);
						begin = char_to_int(begin_offset_buffer);
						string_copy(end_offset_buffer, 0, 4, data_buffer, tuple_offset + c_offset[where_col_index] + 4, tuple_offset + c_offset[where_col_index] + 8);
						end = char_to_int(end_offset_buffer);
						int data_len;
						data_len = end - begin;
						char char_data[128];
						string str;
						string_copy(char_data, 0, data_len, data_buffer, tuple_offset + begin, tuple_offset + end);
						str = char_data;
						string tmp;
						tmp = cons.substr(1, cons.length() - 2);
						if((op == "=" && (str.compare(tmp) == 0)) 
						|| (op == "!=" && (str.compare(tmp) != 0))
						|| (op == "like" && (str.find(tmp) != -1)) 
						|| (op == "not" && (str.find(tmp) == -1))) {
							flag = 1;
						}
					}
					else {
						//cout << "hhhhhhh3333333" <<endl;
						int begin;
						string_copy(begin_offset_buffer, 0, 4, data_buffer, tuple_offset + c_offset[where_col_index], tuple_offset + c_offset[where_col_index] + 4);
						begin = char_to_int(begin_offset_buffer);
						int data_len;
						data_len = tuple_len - begin;
						//cout << "data_len:  " << data_len <<endl;
						char char_data[128];
						string str;
						string_copy(char_data, 0, data_len, data_buffer, tuple_offset + begin, tuple_offset + tuple_len);
						str = char_data;
						string tmp;
						tmp = cons.substr(1, cons.length() - 2);
						if((op == "=" && (str.compare(tmp) == 0)) 
						|| (op == "!=" && (str.compare(tmp) != 0))
						|| (op == "like" && (str.find(tmp) != -1)) 
						|| (op == "not" && (str.find(tmp) == -1))) {
							flag = 1;
						}
					}
	    		}
	    	}
    		// cout << "tuple_len: " << tuple_len <<endl;
    		// cout << "flag: " << flag <<endl;
    		// cout << "c_type[group_col_index]: " << c_type[group_col_index] <<endl;

    		if(flag == 1){ //hash this tuple
    			int group_col_value_int = 0; 
				char group_col_value[128] = {0};
				if(c_type[group_col_index] == 0){//int
	    			int group_col_offset = tuple_offset + c_offset[group_col_index];
	    			//cout << "group_col_offset:  " << group_col_offset <<endl;
	    			
	    			string_copy(group_col_value,0,4,data_buffer, group_col_offset,group_col_offset+4);
	    			group_col_value_int = char_to_int(group_col_value);

	    			//cout << "group_col_value:   " << c <<endl;

	    		}
	    		else{//varchar
					char begin_offset_buffer[4] = {0};
					char end_offset_buffer[4] = {0};
					if(group_col_index != last_var_col_index) {
						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer, tuple_offset + c_offset[group_col_index], tuple_offset + c_offset[group_col_index] + 4);
						begin = char_to_int(begin_offset_buffer);
						string_copy(end_offset_buffer, 0, 4, data_buffer, tuple_offset + c_offset[group_col_index] + 4, tuple_offset + c_offset[group_col_index] + 8);
						end = char_to_int(end_offset_buffer);
						int data_len;
						data_len = end - begin;
						string_copy(group_col_value, 0, data_len, data_buffer, tuple_offset + begin, tuple_offset + end);
					}
					else {

						int begin;
						string_copy(begin_offset_buffer, 0, 4, data_buffer, tuple_offset + c_offset[group_col_index], tuple_offset + c_offset[group_col_index] + 4);
						begin = char_to_int(begin_offset_buffer);
						int data_len;
						data_len = tuple_len - begin;
						string_copy(group_col_value, 0, data_len, data_buffer, tuple_offset + begin, tuple_offset + tuple_len);
					}
	    		}

	    		//cout << "page:  " << i <<endl;
	    		// cout << "group_col_value_int:   " << group_col_value_int <<endl;
	    		// cout << "c_type[group_col_index]:   " << c_type[group_col_index] <<endl;
	    		if(no_group){
	    			for(k = 0; k < agg_op.size(); ++k){
						string_copy(int2char_buffer,0,4,data_buffer, tuple_offset+c_offset[agg_col_index[k]], tuple_offset+c_offset[agg_col_index[k]]+4);
						int agg_col_value = char_to_int(int2char_buffer);
						SUM[k] += agg_col_value;
						COUNT[k] += 1;
						double x = ((double)SUM[k]/(double)COUNT[k]);
						AVG[k] = (int)(x+0.5);
						MIN[k] = (agg_col_value < MIN[k])? agg_col_value : MIN[k]; 
						MAX[k] = (agg_col_value > MIN[k])? agg_col_value : MAX[k]; 
  					}
	    		}
	    		else
	    			insertkey(group_col_value, group_col_value_int, i, tuple_offset, c_type[group_col_index]);

	    	}  
    	}
    }

    //group by group_col in the hash table
    if(no_group){
	    for(j = 0; j < agg_op.size(); ++j){
	    	if(agg_op[j] == "sum")
				cout << SUM[j];	
			else if(agg_op[j] == "count")
				cout << COUNT[j];	
			else if(agg_op[j] == "avg")
				cout << AVG[j];
			else if(agg_op[j] == "min")
				cout << MIN[j];	
			else if(agg_op[j] == "max")
				cout << MAX[j];
			if(j < agg_op.size()-1)
				cout << "|" <<endl;
			else
				cout << endl; 
		} 
    }
    else{	//have group
	    for(i = 0; i < HASHSIZE; ++i){

	    	while(HASHTABLE[i] != NULL){
	    		struct NODE * tmp_node = HASHTABLE[i];
	    		char tmp_key[128] = {0}; 
	    		int tmp_key_int,tmp_page,tmp_offset,tmp_type;

	    		strcpy(tmp_key,tmp_node->key); 		
	    		// cout << "tmp_node->key: " << tmp_node->key <<endl;
	    		// cout << "tmp_key_int: " << tmp_node->key_int <<endl;
	    		tmp_key_int = tmp_node->key_int;
	    		tmp_page = tmp_node->page;
	    		tmp_offset = tmp_node->offset;	
	    		tmp_type = tmp_node->type;
				//cout << "i:   " << i <<endl;
	    		// cout << "tmp_page: " << tmp_page << endl;
	    		// cout << "tmp_offset: " << tmp_offset << endl;
	    		// cout << "tmp_type: " << tmp_type << endl;

	    		char data_buffer[4096] = {0};
				fseek(fp, tmp_page*PAGE_SIZE, SEEK_SET);
				fread(data_buffer, sizeof(char),sizeof(data_buffer),fp);

				if(c_type[group_col_index] == 0){
					string_copy(int2char_buffer,0,4,data_buffer, tmp_offset+c_offset[group_col_index], tmp_offset+c_offset[group_col_index]+4);
					int int_group_col_value = char_to_int(int2char_buffer);
					cout << int_group_col_value;
					if(agg_op.size() > 0)
	    				cout << "|";
	    			else
	    				cout << endl;
				}
				else{
					string_copy(int2char_buffer,0,4,data_buffer, tmp_offset+c_offset[group_col_index], tmp_offset+c_offset[group_col_index]+4);
					int col_start_addr = char_to_int(int2char_buffer);
					int col_end_addr;
					if(c_offset[group_col_index] == last_var_col_index){
						string_copy(int2char_buffer,0,4,data_buffer, tmp_offset, tmp_offset+4);
						int tuple_len = char_to_int(int2char_buffer);
						col_end_addr = tuple_len;
					}
					else{
						string_copy(int2char_buffer,0,4,data_buffer,tmp_offset+c_offset[group_col_index]+4, tmp_offset+c_offset[group_col_index]+8);
						col_end_addr = char_to_int(int2char_buffer);
					}

					char var_group_col_value[128] = {0};
					string_copy(var_group_col_value,0,col_end_addr-col_start_addr,data_buffer, tmp_offset+col_start_addr, tmp_offset+col_end_addr);
					cout << var_group_col_value;
					if(agg_op.size() > 0)
	    				cout << "|";
	    			else
	    				cout << endl;	
				}
				vector <int> SUM,COUNT,AVG,MIN,MAX;
			    for(j = 0; j < agg_op.size(); ++j){
					string_copy(int2char_buffer,0,4,data_buffer, tmp_offset+c_offset[agg_col_index[j]], tmp_offset+c_offset[agg_col_index[j]]+4);
					int agg_col_value = char_to_int(int2char_buffer);
					SUM.push_back(agg_col_value);
					COUNT.push_back(1);
					double x = ((double)SUM[j]/(double)COUNT[j]);
					AVG.push_back((int)(x+0.5));
					MIN.push_back(agg_col_value); 
					MAX.push_back(agg_col_value);
				}
		
	    		// cout << "tmp_node->key: " << tmp_node->key <<endl;
	    		// cout << "tmp_key_int: " << tmp_key_int <<endl;
	    		// cout << "tmp_type: " << tmp_type <<endl;
				if(deletekey(tmp_key,tmp_key_int,tmp_type)){
					//cout << "successfully delete the node" << endl;
					//return;
				}

				struct NODE * next_equal_node = lookupkey(tmp_key,tmp_key_int,tmp_type);
				while(next_equal_node){
					int next_equal_key_int,next_equal_page,next_equal_offset,next_equal_type;

					next_equal_key_int = next_equal_node->key_int;
					next_equal_page = next_equal_node->page;
					next_equal_offset = next_equal_node->offset;
					next_equal_type = next_equal_node->type;

	    			fseek(fp, next_equal_page*PAGE_SIZE, SEEK_SET);
	    			fread(data_buffer, sizeof(char),sizeof(data_buffer),fp);
	    			for(j = 0; j < agg_op.size(); ++j){
		    			string_copy(int2char_buffer,0,4,data_buffer, next_equal_offset+c_offset[agg_col_index[j]], next_equal_offset+c_offset[agg_col_index[j]]+4);
		    			int next_equal_value = char_to_int(int2char_buffer);	
						SUM[j] += next_equal_value;
						COUNT[j] += 1;
						double x = ((double)SUM[j]/(double)COUNT[j]);
						AVG[j] = (int)(x+0.5);
						MIN[j] = (next_equal_value < MIN[j])? next_equal_value : MIN[j]; 
						MAX[j] = (next_equal_value > MIN[j])? next_equal_value : MAX[j]; 
					}
		    		// cout << "tmp_key: " << tmp_key <<endl;
		    		// cout << "tmp_key_int: " << tmp_key_int <<endl;
		    		// cout << "tmp_type: " << tmp_type <<endl;
		    		// cout << "MAX[0]:  " << MAX[0] <<endl;
					deletekey(tmp_key,tmp_key_int,tmp_type);

					next_equal_node = lookupkey(tmp_key,tmp_key_int,tmp_type);
				}			
			
			    for(j = 0; j < agg_op.size(); ++j){
			    	if(agg_op[j] == "sum")
						cout << SUM[j];	
					else if(agg_op[j] == "count")
						cout << COUNT[j];	
					else if(agg_op[j] == "avg")
						cout << AVG[j];
					else if(agg_op[j] == "min")
						cout << MIN[j];	
					else if(agg_op[j] == "max")
						cout << MAX[j];

					if(j < agg_op.size()-1)
						cout << "|" <<endl;
					else
						cout << endl;
				}
	    	}
	    }
	}
    clean();
    return;
}

int cmp_keyword(string name){
    int i,j;
    vector <string> keyword;
    keyword.push_back("int");
    keyword.push_back("varchar");
    keyword.push_back("create");
    keyword.push_back("table");
    keyword.push_back("drop");
    keyword.push_back("insert");
    keyword.push_back("into");
    keyword.push_back("values");
    keyword.push_back("select");
    keyword.push_back("from");
    keyword.push_back("where"); 
    keyword.push_back("and");
    keyword.push_back("group");
    keyword.push_back("by");
    
    for(i=0; i < keyword.size();i++){
        if(strcmp(keyword[i].c_str(),name.c_str())==0) //equal keyword
            return 1;
    }
    return 0;
}

char* int_to_char(int n){
    unsigned int num = n;
    memset(a,0,4);
    a[3] = num%256;
    num /= 256;
    a[2] = num%256;
    num /= 256;
    a[1] = num%256;
    num /= 256; 
    a[0] = num%256;
    return a;
}

int char_to_int(char p[4]){
    int num;
    unsigned char up[4];
    up[0]=p[0]; up[1]=p[1]; up[2]=p[2]; up[3]=p[3];
    num = up[0]*(256*256*256) + up[1]*(256*256) + up[2]*256 + up[3];
    return num;
}

int string_copy(char *dst,int i1,int j1,char *src,int i2,int j2){
    if((j1-i1)!=(j2-i2)){
        cout << "string_copy wrong" <<endl;
        return 0;
    }
    int i;
    for(i=0;i<j1-i1;i++){
        dst[i1+i] = src[i2+i];
    }
    return i;
}