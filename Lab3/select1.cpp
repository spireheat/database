#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
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

void select1(vector <string> col_name, string name, string where_col, string op, string cons){
	int i,j;
	if(cmp_keyword(name)){
		cout << "Syntax error" << endl;
	    return;
	}
	for(i=0;i<col_name.size();++i){
	    if(cmp_keyword(col_name[i])){
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

    //if the column exists
    vector <int> col_index;
    for(j=0;j<col_name.size();j++){
    	col_index.push_back(-1);
    	for(i=0;i<sum;i++){
    		if(c_name[i] == col_name[j])
    			col_index[j] = i;
    	}
    	if(col_index[j] == -1 && col_name[j] != "*"){
    		printf("Column %s doesn’t exist\n",col_name[j].c_str());
    		return;
    	}
    }
    //column and op mismatch error
	int where_col_index = -1; 
    
    int no_where = 0;
    if(where_col.length()==0 && op.length()==0 && cons.length()==0){
    	no_where = 1;
    }
    else{
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

    //print out the first line
    if(col_name[0]=="*"){
	    for(j = 0; j < c_name.size(); j++){
	    	if( j < c_name.size() -1)
	    		cout << c_name[j] << "|";
	    	else
	    		cout << c_name[j] << endl;
	    }
    }
    else{
	    for(j = 0; j < col_name.size(); j++){
	    	if( j < col_name.size() -1)
	    		cout << col_name[j] << "|";
	    	else
	    		cout << col_name[j] << endl;
	    }
	}

    //select the tuple by the op
    char int2char_buffer[4] = {0};
    char data_buffer[4096] = {0};
    int k,flag = 0;
    for(i = 1; i < (fsize/PAGE_SIZE); i++){
    	int page_start_addr = i * PAGE_SIZE;
    	fseek(fp, page_start_addr, SEEK_SET);
    	fread(data_buffer, sizeof(char),sizeof(data_buffer),fp);
    	
    	string_copy(int2char_buffer,0,4,data_buffer,0,4);
    	int full = char_to_int(int2char_buffer);
      	string_copy(int2char_buffer,0,4,data_buffer,4,8);
    	int tuple_count = char_to_int(int2char_buffer);
    	  	
    	for(j = 0; j < tuple_count; j++){
    		int tuple_offset;
    		string_copy(int2char_buffer,0,4,data_buffer,(2+j)*4,(3+j)*4);
    		tuple_offset = char_to_int(int2char_buffer);

    		int tuple_len;
    		string_copy(int2char_buffer,0,4,data_buffer,tuple_offset,tuple_offset+4);
    		tuple_len = char_to_int(int2char_buffer);
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

						int begin;
						string_copy(begin_offset_buffer, 0, 4, data_buffer, tuple_offset + c_offset[where_col_index], tuple_offset + c_offset[where_col_index] + 4);
						begin = char_to_int(begin_offset_buffer);
						int data_len;
						data_len = tuple_len - begin;
						char char_data[128];
						string str;
						string_copy(char_data, 0, data_len, data_buffer, tuple_offset + begin, tuple_offset + tuple_len);
						str = char_data;
						string tmp;
						tmp = cons.substr(1, cons.length() - 2);
						// cout << op <<endl;
						// cout << str <<endl;
						// cout << tmp <<endl;
						// cout << str.find(tmp) <<endl;
						if((op == "=" && (str.compare(tmp) == 0)) 
						|| (op == "!=" && (str.compare(tmp) != 0))
						|| (op == "like" && (str.find(tmp) != -1)) 
						|| (op == "not" && (str.find(tmp) == -1))) {
							flag = 1;
						}
					}
	    		}
	    	}
    		if(flag == 1){ //print out this tuple
    			if(col_name[0] == "*"){
	    			for(k = 0; k < c_name.size(); k++){
	    				if(c_type[k] == 0){
							string_copy(int2char_buffer,0,4,data_buffer, tuple_offset+c_offset[k], tuple_offset+c_offset[k]+4);
							int int_col_value = char_to_int(int2char_buffer);
							if(k < c_name.size()-1)
								cout << int_col_value << "|";
							else
								cout << int_col_value << endl;
						}
						else{
							string_copy(int2char_buffer,0,4,data_buffer, tuple_offset+c_offset[k], tuple_offset+c_offset[k]+4);
							int col_start_addr = char_to_int(int2char_buffer);
							int col_end_addr;
							if(k == last_var_col_index){
								col_end_addr = tuple_len;
							}
							else{
								string_copy(int2char_buffer,0,4,data_buffer, tuple_offset+c_offset[k]+4, tuple_offset+c_offset[k]+8);
								col_end_addr = char_to_int(int2char_buffer);
							}
							// cout << "col_start_addr" << col_start_addr <<endl;
							// cout << "col_end_addr" << col_end_addr <<endl;

							char var_col_value[128] = {0};
							string_copy(var_col_value,0,col_end_addr-col_start_addr,data_buffer, tuple_offset+col_start_addr, tuple_offset+col_end_addr);
				    		if(k < c_name.size()-1)
								cout << var_col_value << "|";
							else
								cout << var_col_value << endl;			
						}
					}    				
    			}
    			else{
	    			for(k = 0; k < col_index.size(); k++){
	    				if(c_type[col_index[k]] == 0){
							string_copy(int2char_buffer,0,4,data_buffer, tuple_offset+c_offset[col_index[k]], tuple_offset+c_offset[col_index[k]]+4);
							int int_col_value = char_to_int(int2char_buffer);
							if(k < col_index.size()-1)
								cout << int_col_value << "|";
							else
								cout << int_col_value << endl;
						}
						else{
							string_copy(int2char_buffer,0,4,data_buffer, tuple_offset+c_offset[col_index[k]], tuple_offset+c_offset[col_index[k]]+4);
							int col_start_addr = char_to_int(int2char_buffer);
							int col_end_addr;
							if(col_index[k] == last_var_col_index){
								col_end_addr = tuple_len;
							}
							else{
								string_copy(int2char_buffer,0,4,data_buffer, tuple_offset+c_offset[col_index[k]]+4, tuple_offset+c_offset[col_index[k]]+8);
								col_end_addr = char_to_int(int2char_buffer);
							}
							// cout << "col_start_addr" << col_start_addr <<endl;
							// cout << "col_end_addr" << col_end_addr <<endl;

							char var_col_value[128] = {0};
							string_copy(var_col_value,0,col_end_addr-col_start_addr,data_buffer, tuple_offset+col_start_addr, tuple_offset+col_end_addr);
				    		if(k < col_index.size()-1)
								cout << var_col_value << "|";
							else
								cout << var_col_value << endl;			
						}
					} 
				}  			
    		}
    	}
    }
    return;
}

int file_size2(char* filename){  
    struct stat statbuf;  
    stat(filename,&statbuf);  
    int size=statbuf.st_size;
    return size;  
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