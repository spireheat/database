#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <limits.h>
#include "hash.cpp"
#include "hash2.cpp"
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

void select4(string col_name, int col_tbl, vector <string> agg_op,vector <string> agg_col, vector <int> agg_tbl,
	vector <string> name, 
	vector <string> where_col, vector <int> where_tbl, vector <string> op, vector <string> cons, 
	vector <string> join_col, vector <int> join_tbl,
	string group_col, int group_tbl){
	FILE * fp[2];
	int i, j;

	if(cmp_keyword(col_name) || cmp_keyword(group_col))
	{
		cout << "Syntax error" << endl;
	       return;
	}	

	for(i = 0; i < agg_col.size(); i++)
	{
		if(cmp_keyword(agg_col[i]))
		{
			cout << "Syntax error" << endl;
	        return;
		}
	}
	for(i = 0; i < name.size(); i++)
	{
		if(cmp_keyword(name[i]))
		{
			cout << "Syntax error" << endl;
	        return;
		}
	}
	for(i = 0; i < where_col.size(); i++)
	{
		if(cmp_keyword(where_col[i]))
		{
			cout << "Syntax error" << endl;
	        return;
		}
	}
	for(i = 0; i < join_col.size(); i++)
	{
		if(cmp_keyword(join_col[i]))
		{
			cout << "Syntax error" << endl;
	        return;
		}
	}
	char path[50];
    sprintf(path,"db/%s.tbl",name[0].c_str());
    fp[0]=fopen(path,"r");
    if(fp[0]==NULL){   //don't have existed before
        printf("Table %s doesn't exist\n",name[0].c_str());
        return;
    }
    int fsize1 = file_size2(path);

    sprintf(path,"db/%s.tbl",name[1].c_str());
    fp[1]=fopen(path,"r");
    if(fp[1]==NULL){   //don't have existed before
        printf("Table %s doesn't exist\n",name[1].c_str());
        return;
    }
    int fsize2 = file_size2(path);
	if(fsize1 <= fsize2)
		;
	else
	{
		int tmp = fsize1;
		fsize2 = fsize1;
		fsize1 = fsize2;
		sprintf(path,"db/%s.tbl",name[1].c_str());
		fp[0]=fopen(path,"r");
		sprintf(path,"db/%s.tbl",name[0].c_str());
		fp[1]=fopen(path,"r");
		if(col_tbl == 1)
			col_tbl = 2;
		else if(col_tbl == 2)
			col_tbl = 1;

		if(group_tbl == 1)
			group_tbl = 2;
		else if(group_tbl == 2)
			group_tbl = 1;
		
		for(i = 0; i < where_col.size(); i++)
		{
			if(where_tbl[i] == 1)
				where_tbl[i] = 2;
			else if(where_tbl[i] == 2)
				where_tbl[i] = 1;
		}
		for(i = 0; i < agg_col.size(); i++)
		{
			if(agg_tbl[i] == 1)
				agg_tbl[i] = 2;
			else if(agg_tbl[i] == 2)
				agg_tbl[i] = 1;
		}
		for(i = 0; i < join_col.size(); i++)
		{
			if(join_tbl[i] == 1)
				join_tbl[i] = 2;
			else if(join_tbl[i] == 2)
				join_tbl[i] = 1;
		}
	}
	
	//get tab1 head information
    int sum1,fix1,var1,last_var_col_index1;
	fscanf(fp[0],"%d,%d,%d,%d",&sum1,&fix1,&var1,&last_var_col_index1);
		
    vector <string> c_name1;
    vector <int> c_type1;
    vector <int> c_offset1;
    int start_addr1;
    for(i=0;i<sum1;i++){
        char c_name_buffer[128]={0};
        int type;
        int offset;

        start_addr1 = 46 + i*150;
        fseek(fp[0], start_addr1, SEEK_SET);
        fscanf(fp[0],"%s",c_name_buffer);
        c_name1.push_back(c_name_buffer);

        start_addr1 = 46 + i*150 + 128;
        fseek(fp[0], start_addr1, SEEK_SET);
        fscanf(fp[0],"%d,%d",&type,&offset);
        c_type1.push_back(type);
        c_offset1.push_back(offset);
    }
	
	//get tab2 head information
    int sum2,fix2,var2,last_var_col_index2;
	fscanf(fp[1],"%d,%d,%d,%d",&sum2,&fix2,&var2,&last_var_col_index2);
		
    vector <string> c_name2;
    vector <int> c_type2;
    vector <int> c_offset2;
    int start_addr2;
    for(i=0;i<sum2;i++){
        char c_name_buffer[128]={0};
        int type;
        int offset;

        start_addr2 = 46 + i*150;
        fseek(fp[1], start_addr2, SEEK_SET);
        fscanf(fp[1],"%s",c_name_buffer);
        c_name2.push_back(c_name_buffer);

        start_addr2 = 46 + i*150 + 128;
        fseek(fp[1], start_addr2, SEEK_SET);
        fscanf(fp[1],"%d,%d",&type,&offset);
        c_type2.push_back(type);
        c_offset2.push_back(offset);
    }


	//Non-group-by column in select list
	if(col_name != group_col){
		printf("Non-group-by column %s in select list\n",col_name.c_str());
		return;
	}

	int no_group = 0;
	if(group_col == "")
		no_group = 1;

	//if the sel_col exists and get col_index
	int col_index= -1;
	if(col_tbl == 0)
	{
		for(i=0;i<sum1;i++){
    		if(c_name1[i] == col_name)
    			col_index = i;
		}
		//cout << "col_index:  " << col_index <<endl;
		if(col_index == -1){
			for(i=0;i<sum2;i++){
				if(c_name2[i] == col_name)
				{
					col_index = i;
					col_tbl = 2;
				}
			}
			if(col_index == -1 && no_group == 0){
				printf("Column %s doesn't exist\n",col_name.c_str());
				return;
			}
		}
		else
		{
			int tmp = -1;
			for(i=0;i<sum2;i++){
				if(c_name2[i] == col_name)
					tmp = i;
			}
			if(tmp == -1){
				col_tbl = 1;
			}
			else
			{
				cout << "Ambiguous column " << col_name << endl;
				return;
			}
		}
	}
	else if(col_tbl == 1)
	{
		for(i=0;i<sum1;i++){
			if(c_name1[i] == col_name)
				col_index = i;
		}
		if(col_index == -1 && no_group == 0){
			printf("Column %s doesn't exist\n",col_name.c_str());
			return;
		}
	}
	else//col_tbl = 2
	{
		for(i=0;i<sum2;i++){
			if(c_name2[i] == col_name)
				col_index = i;
		}
		if(col_index == -1 && no_group == 0){
			printf("Column %s doesn't exist\n",col_name.c_str());
			return;
		}
	}


	//if the group_col exists and get group_col_index
	int group_col_index= -1;
	if(group_tbl == 0)
	{
		for(i=0;i<sum1;i++){
    		if(c_name1[i] == group_col)
    			group_col_index = i;
		}
		//cout << "col_index:  " << col_index <<endl;
		if(group_col_index == -1){
			for(i=0;i<sum2;i++){
				if(c_name2[i] == group_col)
				{
					group_col_index = i;
					group_tbl = 2;
				}
			}
			if(group_col_index == -1 && no_group == 0){
				printf("Column %s doesn't exist\n",group_col.c_str());
				return;
			}
		}
		else
		{
			int tmp = -1;
			for(i=0;i<sum2;i++){
				if(c_name2[i] == group_col)
					tmp = i;
			}
			if(tmp == -1){
				group_tbl = 1;
			}
			else	//this column exists in two table
			{
				cout << "Ambiguous column " << group_col << endl;
				return;
			}
		}
	}
	else if(group_tbl == 1)
	{
		for(i=0;i<sum1;i++){
			if(c_name1[i] == group_col)
				group_col_index = i;
		}
		if(group_col_index == -1 && no_group == 0){
			printf("Column %s doesn't exist\n",group_col.c_str());
			return;
		}
	}
	else//group_tbl = 2
	{
		for(i=0;i<sum2;i++){
			if(c_name2[i] == group_col)
				group_col_index = i;
		}
		if(group_col_index == -1 && no_group == 0){
			printf("Column %s doesn't exist\n",group_col.c_str());
			return;
		}
	}

	// if the agg_col exists and get agg_col_index
	vector <int> agg_col_index;
    for(j=0;j < agg_col.size();j++){
    	agg_col_index.push_back(-1);
		//cout << "agg_tbl[j]: " << agg_tbl[j] <<endl;
		if(agg_tbl[j] == 0)
		{
			for(i=0;i<sum1;i++){
	    		if(c_name1[i] == agg_col[j])
	    			agg_col_index[j] = i;
			}
			//cout << "agg_col_index[j]:  " << agg_col_index[j] <<endl;
			if(agg_col[j] == "*")
			{
				;
			}
			else if(agg_col_index[j] == -1){
				for(i=0;i<sum2;i++){
					if(c_name2[i] == agg_col[j])
					{
						agg_col_index[j] = i;
						agg_tbl[j] = 2;
					}
				}
				if(agg_col_index[j] == -1 && agg_col[j] != "*"){
					printf("Column %s doesn't exist\n",agg_col[j].c_str());
					return;
				}
			}
			else
			{
				int tmp = -1;
				for(i=0;i<sum2;i++){
					if(c_name2[i] == agg_col[j])
						tmp = i;
				}
				if(tmp == -1){
					agg_tbl[j] = 1;
				}
				else
				{
					cout << "Ambiguous column " << agg_col[j] << endl;
					return;
				}
			}
		}
		else if(agg_tbl[j] == 1)
		{
			for(i=0;i<sum1;i++){
    		if(c_name1[i] == agg_col[j])
    			agg_col_index[j] = i;
			}
			if(agg_col_index[j] == -1 && agg_col[j] != "*"){
				printf("Column %s doesn't exist\n",agg_col[j].c_str());
				return;
			}
		}
		else
		{
			for(i=0;i<sum2;i++){
				if(c_name2[i] == agg_col[j])
					agg_col_index[j] = i;
			}
			if(agg_col_index[j] == -1 && agg_col[j] != "*"){
				printf("Column %s doesn't exist\n",agg_col[j].c_str());
				return;
			}
		}
    }

    //agg_col is not int
	for(j=0;j<agg_col_index.size();j++){
		if(agg_col[j] == "*" && (agg_op[j] == "sum" || agg_op[j] == "avg" 
				|| agg_op[j] == "min" || agg_op[j] == "max")){
			printf("Column %s is not int and can’t be used in aggregation\n", agg_col[j].c_str());
			return;
		}
		if(agg_tbl[j] == 1){
			if(c_type1[agg_col_index[j]] == 1 && (agg_op[j] == "sum" || agg_op[j] == "avg" 
				|| agg_op[j] == "min" || agg_op[j] == "max")){
				printf("Column %s is not int and can’t be used in aggregation\n", agg_col[j].c_str());
				return;
			}
		}
		else{
			if(c_type2[agg_col_index[j]] == 1 && (agg_op[j] == "sum" || agg_op[j] == "avg" 
				|| agg_op[j] == "min" || agg_op[j] == "max")){
				printf("Column %s is not int and can’t be used in aggregation\n", agg_col[j].c_str());
				return;
			}			
		}
	}

	//if the where_col exists and get where_col_index and judge where_col and op mismatch error
	vector <int> where_col_index;
	for(i = 0; i < where_col.size(); i++)
		where_col_index.push_back(-1);

    int no_where = 0;
    if(where_col.size()==0 && op.size()==0 && cons.size()==0){
    	no_where = 1;
    }
    else{
		for(j = 0; j < where_col.size(); j++)
		{
			if(where_tbl[j] == 0)
			{
				for(i=0;i<sum1;i++){
					if(c_name1[i] == where_col[j])
					{
						where_col_index[j] = i;
						where_tbl[j] = 1;
					}
				}

				if(where_col_index[j] == -1){
					for(i=0;i<sum2;i++){
						if(c_name2[i] == where_col[j])
						{
							where_col_index[j] = i;
							where_tbl[j] = 2;
						}
					}
					if(where_col_index[j] == -1){
						printf("Column %s doesn't exist\n",where_col[j].c_str());
						return;
					}
				}
				else
				{
					int tmp = -1;
					for(i=0;i<sum2;i++){
						if(c_name2[i] == where_col[j])
							tmp = i;
					}
					if(tmp == -1){
						where_tbl[j] = 1;
					}
					else
					{
						cout << "Ambiguous column " << where_col[j] << endl;
						return;
					}
				}
			}
			else if(where_tbl[j] == 1)
			{
				for(i=0;i<sum1;i++){
					if(c_name1[i] == where_col[j])
					{
						where_col_index[j] = i;
						where_tbl[j] = 1;
					}
				}
				if(where_col_index[j] == -1){
					printf("Column %s doesn't exist\n",where_col[j].c_str());
					return;
				}
			}
			else
			{
				for(i=0;i<sum2;i++){
					if(c_name2[i] == where_col[j])
					{
						where_col_index[j] = i;
						where_tbl[j] = 2;
					}
				}
				if(where_col_index[j] == -1){
					printf("Column %s doesn't exist\n",where_col[j].c_str());
					return;
				}
			}
		}
		for(j = 0; j < where_col.size(); j++)
		{
			if(where_tbl[j] == 1)
			{
				if(c_type1[where_col_index[j]] == 0) //int type
				{
					if(op[j] == "like" || op[j] == "not" || cons[j][0] == '\'')
					{
						cout << "Predicate " << where_col[j] << " " << op[j] << " " << cons[j] << " error" << endl;
						return;
					}
				}
				else
				{
					if(op[j] == "<" || op[j] == "<=" || op[j] == ">" || op[j] == ">=" || cons[j][0] != '\'')
					{
						cout << "Predicate " << where_col[j] << " " << op[j] << " " << cons[j] << " error" << endl;
						return;
					}
				}
			}
			else
			{
				if(c_type2[where_col_index[j]] == 0) //int type
				{
					if(op[j] == "like" || op[j] == "not" || cons[j][0] == '\'')
					{
						cout << "Predicate " << where_col[j] << " " << op[j] << " " << cons[j] << " error" << endl;
						return;
					}
				}
				else
				{
					if(op[j] == "<" || op[j] == "<=" || op[j] == ">" || op[j] == ">=" || cons[j][0] != '\'')
					{
						cout << "Predicate " << where_col[j] << " " << op[j] << " " << cons[j] << " error" << endl;
						return;
					}
				}
			}
		}
	}
		
	//if the join_col exists and get join_col_index and judge "join predicate error"
	vector <int> join_col_index;
	for(j = 0; j < join_col.size(); j++)
		join_col_index.push_back(-1);
	for(j = 0; j < join_col.size(); j++)
	{
		if(join_tbl[j] == 0)
		{
			for(i=0;i<sum1;i++){
				if(c_name1[i] == join_col[j])
				{
					join_col_index[j] = i;
					join_tbl[j] = 1;
				}
			}
			if(join_col_index[j] == -1){
				for(i=0;i<sum2;i++){
					if(c_name2[i] == join_col[j])
					{
						join_col_index[j] = i;
						join_tbl[j] = 2;
					}
				}
				if(join_col_index[j] == -1){
					printf("Column %s doesn't exist\n",join_col[j].c_str());
					return;
				}
			}
		}
		else if(join_tbl[j] == 1)
		{
			for(i=0;i<sum1;i++){
				if(c_name1[i] == join_col[j])
				{
					join_col_index[j] = i;
					join_tbl[j] = 1;
				}
			}
			if(join_col_index[j] == -1){
				printf("Column %s doesn't exist\n",join_col[j].c_str());
				return;
			}
		}
		else
		{
			for(i=0;i<sum2;i++){
				if(c_name2[i] == join_col[j])
				{
					join_col_index[j] = i;
					join_tbl[j] = 2;
				}
			}
			if(join_col_index[j] == -1){
				printf("Column %s doesn't exist\n",join_col[j].c_str());
				return;
			}
		}
	}
	if(join_tbl[0] == 2)
	{
		string tmp = join_col[0];
		join_col[0] = join_col[1];
		join_col[1] = tmp;
		join_tbl[0] = 1;
		join_tbl[1] = 2;
		int temp;
		temp = join_col_index[0];
		join_col_index[0] = join_col_index[1];
		join_col_index[1] = temp;
	}
 	if(c_type1[join_col_index[0]] != c_type2[join_col_index[1]])
	{
		cout << "Join predicate error" << endl;
		return;
	}
	
	//use hash to join
	char int2char_buffer[4] = {0};
    char data_buffer1[4096] = {0};
	char data_buffer2[4096] = {0};
    int k,flag = 0;
	int where_col_index_tab1 = -1;
	int where_col_index_tab2 = -1;
	for(i = 0; i < where_col.size(); i++)
	{
		if(where_tbl[i] == 1)
			where_col_index_tab1 = i;
		else
			where_col_index_tab2 = i;
	}

	for(i = 1; i < (fsize1/PAGE_SIZE); i++)
	{
		int page_start_addr1 = i * PAGE_SIZE;
		fseek(fp[0], page_start_addr1, SEEK_SET);
		fread(data_buffer1, sizeof(char),sizeof(data_buffer1),fp[0]);
		
		string_copy(int2char_buffer,0,4,data_buffer1,0,4);
		int full1 = char_to_int(int2char_buffer);
		string_copy(int2char_buffer,0,4,data_buffer1,4,8);
		int tuple_count1 = char_to_int(int2char_buffer);
		
		for(j = 0; j < tuple_count1; j++)
		{
			int tuple_tuple_offset1;
			string_copy(int2char_buffer,0,4,data_buffer1,(2+j)*4,(3+j)*4);
			tuple_tuple_offset1 = char_to_int(int2char_buffer);

			int tuple_len1;
			string_copy(int2char_buffer,0,4,data_buffer1,tuple_tuple_offset1,tuple_tuple_offset1+4);
			tuple_len1 = char_to_int(int2char_buffer);
			flag = 0;
			if(where_col_index_tab1 == -1)
				flag = 1;
			else
			{
				if(c_type1[where_col_index[where_col_index_tab1]] == 0) //int
				{
					int where_col_value1;
					int where_col_tuple_offset1 = tuple_tuple_offset1 + c_offset1[where_col_index[where_col_index_tab1]];
					string_copy(int2char_buffer,0,4,data_buffer1, where_col_tuple_offset1,where_col_tuple_offset1+4);
					where_col_value1 = char_to_int(int2char_buffer);

					char cons_value_buffer[200] = {0};
					cons[where_col_index_tab1].copy(cons_value_buffer, cons[where_col_index_tab1].length(), 0);
					int const_value1 = atoi(cons_value_buffer);
					if(	(op[where_col_index_tab1] == "<" && where_col_value1 < const_value1)
						|| (op[where_col_index_tab1] == "<=" && where_col_value1 <= const_value1)
						|| (op[where_col_index_tab1] == ">" && where_col_value1 > const_value1)
						|| (op[where_col_index_tab1] == ">=" && where_col_value1 >= const_value1)
						|| (op[where_col_index_tab1] == "=" && where_col_value1 == const_value1)
						|| (op[where_col_index_tab1] == "!=" && where_col_value1 != const_value1) )
					{
						flag = 1;
					}
				}
				else //varchar
				{
					char begin_offset_buffer[4] = {0};
					char end_offset_buffer[4] = {0};

					if(where_col_index[where_col_index_tab1] != last_var_col_index1){
						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer1, tuple_tuple_offset1 + c_offset1[where_col_index[where_col_index_tab1]], tuple_tuple_offset1 + c_offset1[where_col_index[where_col_index_tab1]] + 4);
						begin = char_to_int(begin_offset_buffer);
						string_copy(end_offset_buffer, 0, 4, data_buffer1, tuple_tuple_offset1 + c_offset1[where_col_index[where_col_index_tab1]] + 4, tuple_tuple_offset1 + c_offset1[where_col_index[where_col_index_tab1]] + 8);
						end = char_to_int(end_offset_buffer);
						int data_len;
						data_len = end - begin;
						char char_data[128];
						string str;
						string_copy(char_data, 0, data_len, data_buffer1, tuple_tuple_offset1 + begin, tuple_tuple_offset1 + end);
						str = char_data;
						string tmp;
						tmp = cons[where_col_index_tab1].substr(1, cons[where_col_index_tab1].length() - 2);
						if((op[where_col_index_tab1] == "=" && (str.compare(tmp) == 0)) 
						|| (op[where_col_index_tab1] == "!=" && (str.compare(tmp) != 0))
						|| (op[where_col_index_tab1] == "like" && (str.find(tmp) != -1)) 
						|| (op[where_col_index_tab1] == "not" && (str.find(tmp) == -1)))
						{
							flag = 1;
						}
					}
					else {
						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer1, tuple_tuple_offset1 + c_offset1[where_col_index[where_col_index_tab1]], tuple_tuple_offset1 + c_offset1[where_col_index[where_col_index_tab1]] + 4);
						begin = char_to_int(begin_offset_buffer);
						end = tuple_len1;
						int data_len;
						data_len = end - begin;
						char char_data[128];
						string str;
						string_copy(char_data, 0, data_len, data_buffer1, tuple_tuple_offset1 + begin, tuple_tuple_offset1 + end);
						str = char_data;
						string tmp;
						tmp = cons[where_col_index_tab1].substr(1, cons[where_col_index_tab1].length() - 2);
						if((op[where_col_index_tab1] == "=" && (str.compare(tmp) == 0)) 
						|| (op[where_col_index_tab1] == "!=" && (str.compare(tmp) != 0))
						|| (op[where_col_index_tab1] == "like" && (str.find(tmp) != -1)) 
						|| (op[where_col_index_tab1] == "not" && (str.find(tmp) == -1)))
						{
							flag = 1;
						}
					}
				}
			}
			
			
			
			if(flag == 1){ //hash this tuple
    			int join_col_value_int = 0; 
				char join_col_value[128] = {0};
				if(c_type1[join_col_index[0]] == 0){//int
	    			int join_col_offset = tuple_tuple_offset1 + c_offset1[join_col_index[0]];
	    			//cout << "group_col_offset:  " << group_col_offset <<endl;
	    			
	    			string_copy(join_col_value,0,4,data_buffer1, join_col_offset,join_col_offset+4);
	    			join_col_value_int = char_to_int(join_col_value);

	    			//cout << "group_col_value:   " << c <<endl;

	    		}
	    		else{//varchar
					char begin_offset_buffer[4] = {0};
					char end_offset_buffer[4] = {0};
					if(join_col_index[0] != last_var_col_index1) {
						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer1, tuple_tuple_offset1 + c_offset1[join_col_index[0]], tuple_tuple_offset1 + c_offset1[join_col_index[0]] + 4);
						begin = char_to_int(begin_offset_buffer);
						string_copy(end_offset_buffer, 0, 4, data_buffer1, tuple_tuple_offset1 + c_offset1[join_col_index[0]] + 4, tuple_tuple_offset1 + c_offset1[join_col_index[0]] + 8);
						end = char_to_int(end_offset_buffer);
						int data_len;
						data_len = end - begin;
						string_copy(join_col_value, 0, data_len, data_buffer1, tuple_tuple_offset1 + begin, tuple_tuple_offset1 + end);
					}
					else {

						int begin;
						string_copy(begin_offset_buffer, 0, 4, data_buffer1, tuple_tuple_offset1 + c_offset1[join_col_index[0]], tuple_tuple_offset1 + c_offset1[join_col_index[0]] + 4);
						begin = char_to_int(begin_offset_buffer);
						int data_len;
						data_len = tuple_len1 - begin;
						string_copy(join_col_value, 0, data_len, data_buffer1, tuple_tuple_offset1 + begin, tuple_tuple_offset1 + tuple_len1);
					}
	    		}

	    		//cout << "page:  " << i <<endl;
	    		// cout << "group_col_value_int:   " << group_col_value_int <<endl;
	    		// cout << "c_type[group_col_index]:   " << c_type[group_col_index] <<endl; 		
	    		insertkey(join_col_value, join_col_value_int, i, tuple_tuple_offset1, c_type1[join_col_index[0]]);
	    	}
			
		}
	}

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

	vector <int> SUM,COUNT,AVG,MIN,MAX;
	for(j = 0; j < agg_op.size(); ++j){
		SUM.push_back(0);
		COUNT.push_back(0);
		AVG.push_back(0);
		MIN.push_back(INT_MAX); 
		MAX.push_back(INT_MIN);
	}

	//search tab2
	vector <NODE> delete_node;
	for(i = 1; i < (fsize2/PAGE_SIZE); i++)
	{
		int page_start_addr2 = i * PAGE_SIZE;
		fseek(fp[1], page_start_addr2, SEEK_SET);
		fread(data_buffer2, sizeof(char),sizeof(data_buffer2),fp[1]);
		
		string_copy(int2char_buffer,0,4,data_buffer2,0,4);
		int full2 = char_to_int(int2char_buffer);
		string_copy(int2char_buffer,0,4,data_buffer2,4,8);
		int tuple_count2 = char_to_int(int2char_buffer);
		
		for(j = 0; j < tuple_count2; j++)
		{
			int tuple_offset2;
			string_copy(int2char_buffer,0,4,data_buffer2,(2+j)*4,(3+j)*4);
			tuple_offset2 = char_to_int(int2char_buffer);

			int tuple_len2;
			string_copy(int2char_buffer,0,4,data_buffer2,tuple_offset2,tuple_offset2+4);
			tuple_len2 = char_to_int(int2char_buffer);
			flag = 0;
			if(where_col_index_tab2 == -1)
				flag = 1;
			else
			{
				if(c_type2[where_col_index[where_col_index_tab2]] == 0) //int
				{
					int where_col_value2;
					int where_col_offset2 = tuple_offset2 + c_offset2[where_col_index[where_col_index_tab2]];
					string_copy(int2char_buffer,0,4,data_buffer2, where_col_offset2,where_col_offset2+4);
					where_col_value2 = char_to_int(int2char_buffer);

					char cons_value_buffer[200] = {0};
					cons[where_col_index_tab2].copy(cons_value_buffer, cons[where_col_index_tab2].length(), 0);
					int const_value2 = atoi(cons_value_buffer);
					if(	(op[where_col_index_tab2] == "<" && where_col_value2 < const_value2)
						|| (op[where_col_index_tab2] == "<=" && where_col_value2 <= const_value2)
						|| (op[where_col_index_tab2] == ">" && where_col_value2 > const_value2)
						|| (op[where_col_index_tab2] == ">=" && where_col_value2 >= const_value2)
						|| (op[where_col_index_tab2] == "=" && where_col_value2 == const_value2)
						|| (op[where_col_index_tab2] == "!=" && where_col_value2 != const_value2) )
					{
						flag = 1;
					}
				}
				else //varchar
				{
					char begin_offset_buffer[4] = {0};
					char end_offset_buffer[4] = {0};

					if(where_col_index[where_col_index_tab2] != last_var_col_index2){
						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer2, tuple_offset2 + c_offset2[where_col_index[where_col_index_tab2]], tuple_offset2 + c_offset2[where_col_index[where_col_index_tab2]] + 4);
						begin = char_to_int(begin_offset_buffer);
						string_copy(end_offset_buffer, 0, 4, data_buffer2, tuple_offset2 + c_offset2[where_col_index[where_col_index_tab2]] + 4, tuple_offset2 + c_offset2[where_col_index[where_col_index_tab2]] + 8);
						end = char_to_int(end_offset_buffer);
						int data_len;
						data_len = end - begin;
						char char_data[128];
						string str;
						string_copy(char_data, 0, data_len, data_buffer2, tuple_offset2 + begin, tuple_offset2 + end);
						str = char_data;
						string tmp;
						tmp = cons[where_col_index_tab2].substr(1, cons[where_col_index_tab2].length() - 2);
						if((op[where_col_index_tab2] == "=" && (str.compare(tmp) == 0)) 
						|| (op[where_col_index_tab2] == "!=" && (str.compare(tmp) != 0))
						|| (op[where_col_index_tab2] == "like" && (str.find(tmp) != -1)) 
						|| (op[where_col_index_tab2] == "not" && (str.find(tmp) == -1)))
						{
							flag = 1;
						}
					}
					else {
						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer2, tuple_offset2 + c_offset2[where_col_index[where_col_index_tab2]], tuple_offset2 + c_offset2[where_col_index[where_col_index_tab2]] + 4);
						begin = char_to_int(begin_offset_buffer);
						end = tuple_len2;
						int data_len;
						data_len = end - begin;
						char char_data[128];
						string str;
						string_copy(char_data, 0, data_len, data_buffer2, tuple_offset2 + begin, tuple_offset2 + end);
						str = char_data;
						string tmp;
						tmp = cons[where_col_index_tab2].substr(1, cons[where_col_index_tab2].length() - 2);
						if((op[where_col_index_tab2] == "=" && (str.compare(tmp) == 0)) 
						|| (op[where_col_index_tab2] == "!=" && (str.compare(tmp) != 0))
						|| (op[where_col_index_tab2] == "like" && (str.find(tmp) != -1)) 
						|| (op[where_col_index_tab2] == "not" && (str.find(tmp) == -1)))
						{
							flag = 1;
						}
					}
				}
			}
			
			if(flag == 1){ //use hash table to find the same tuple in table1
				char join_col_value[128] = {0};
				int join_col_value_int;
				if(c_type2[join_col_index[1]] == 0){//int
	    			int join_col_offset = tuple_offset2 + c_offset2[join_col_index[1]];
	    		
	    			string_copy(join_col_value,0,4,data_buffer2, join_col_offset,join_col_offset+4);
	    			join_col_value_int = char_to_int(join_col_value);

	    		}
	    		else{//varchar
					char begin_offset_buffer[4] = {0};
					char end_offset_buffer[4] = {0};
					if(join_col_index[1] != last_var_col_index2) {
						int begin, end;
						string_copy(begin_offset_buffer, 0, 4, data_buffer2, tuple_offset2 + c_offset2[join_col_index[1]], tuple_offset2 + c_offset2[join_col_index[1]] + 4);
						begin = char_to_int(begin_offset_buffer);
						string_copy(end_offset_buffer, 0, 4, data_buffer2, tuple_offset2 + c_offset2[join_col_index[1]] + 4, tuple_offset2 + c_offset2[join_col_index[1]] + 8);
						end = char_to_int(end_offset_buffer);
						int data_len;
						data_len = end - begin;
						string_copy(join_col_value, 0, data_len, data_buffer2, tuple_offset2 + begin, tuple_offset2 + end);
					}
					else {
						int begin;
						string_copy(begin_offset_buffer, 0, 4, data_buffer2, tuple_offset2 + c_offset2[join_col_index[1]], tuple_offset2 + c_offset2[join_col_index[1]] + 4);
						begin = char_to_int(begin_offset_buffer);
						int data_len;
						data_len = tuple_len2 - begin;
						string_copy(join_col_value, 0, data_len, data_buffer2, tuple_offset2 + begin, tuple_offset2 + tuple_len2);
					}
	    		}

				struct NODE* next_equal_node = lookupkey(join_col_value,join_col_value_int,c_type2[join_col_index[1]]);
	    		while(next_equal_node)
				{
					int page1,tuple_offset1,page2,offset2;
					page1 = next_equal_node->page;
					tuple_offset1 = next_equal_node->offset;
					fseek(fp[0], page1*PAGE_SIZE, SEEK_SET);
					fread(data_buffer1, sizeof(char),sizeof(data_buffer1),fp[0]);
					if(no_group){
		    			for(k = 0; k < agg_op.size(); ++k){
		    				//cout << "agg_tbl[k]:  "  << agg_tbl[k] <<endl;
		    				//cout << "tuple_offset1: "  << tuple_offset1  <<endl;
							if(agg_tbl[k]==1){
								string_copy(int2char_buffer,0,4, data_buffer1, tuple_offset1 + c_offset1[agg_col_index[k]], tuple_offset1 + c_offset1[agg_col_index[k]]+4);
							}
							else{
								string_copy(int2char_buffer,0,4, data_buffer2, tuple_offset2 + c_offset2[agg_col_index[k]], tuple_offset2 + c_offset2[agg_col_index[k]]+4);
							}	  		
			    			int next_equal_value = char_to_int(int2char_buffer);	
			    			//cout << "next_equal_value:   "   << next_equal_value <<endl;
							SUM[k] += next_equal_value;
							COUNT[k] += 1;
							double x = ((double)SUM[k]/(double)COUNT[k]);
							AVG[k] = (int)(x+0.5);
							MIN[k] = (next_equal_value < MIN[k])? next_equal_value : MIN[k]; 
							MAX[k] = (next_equal_value > MIN[k])? next_equal_value : MAX[k]; 
							//cout << "hhhh"  <<endl;
						}
					}
					else{
						insertkey2(join_col_value, join_col_value_int, page1, tuple_offset1, i, tuple_offset2, c_type2[join_col_index[1]]);
					}
						
					delete_node.push_back(*next_equal_node);
					deletekey(join_col_value,join_col_value_int,c_type2[join_col_index[1]]);
					next_equal_node = lookupkey(join_col_value,join_col_value_int,c_type2[join_col_index[1]]);
				}
				for(k=0;k<delete_node.size();++k){
					insertkey(delete_node[k].key, delete_node[k].key_int, delete_node[k].page, delete_node[k].offset, delete_node[k].type);
				}
				delete_node.clear();
	    	}
		}
	}

	//group by group_col_name in the hash table
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
				cout << "|";
			else
				cout << endl; 
		} 
	}
	else{
	    for(i = 0; i < HASHSIZE; ++i){
	    	while(HASHTABLE2[i] != NULL){
	    		struct NODE2 * tmp_node = HASHTABLE2[i];
	    		//SUM =0 ; COUNT = 0; AVG = 0; MIN = INT_MAX; MAX = INT_MIN;
	    		char tmp_key[128] = {0}; 
	    		int tmp_key_int,tmp_page1,tmp_offset1,tmp_page2,tmp_offset2,tmp_type;

	    		strcpy(tmp_key,tmp_node->key); 		
	    		// cout << "tmp_node->key: " << tmp_node->key <<endl;
	    		// cout << "tmp_key_int: " << tmp_node->key_int <<endl;

	    		tmp_key_int = tmp_node->key_int;
	    		tmp_page1 = tmp_node->page1;
	    		tmp_offset1 = tmp_node->offset1;	
	    		tmp_page2 = tmp_node->page2;
	    		tmp_offset2 = tmp_node->offset2;	
	    		tmp_type = tmp_node->type;

				// cout << "i:   " << i <<endl;
				// cout << "tmp_key_int:   "  <<  tmp_key_int  <<endl;

				fseek(fp[0], tmp_page1 * PAGE_SIZE, SEEK_SET);
				fread(data_buffer1, sizeof(char),sizeof(data_buffer1),fp[0]);

				fseek(fp[1], tmp_page2 * PAGE_SIZE, SEEK_SET);
				fread(data_buffer2, sizeof(char),sizeof(data_buffer2),fp[1]);

				if(group_tbl == 1){
					if(c_type1[group_col_index] == 0){
						string_copy(int2char_buffer,0,4,data_buffer1, tmp_offset1+c_offset1[group_col_index], tmp_offset1+c_offset1[group_col_index]+4);
						int int_group_col_value = char_to_int(int2char_buffer);
						cout << int_group_col_value;
						if(agg_op.size() > 0)
		    				cout << "|";
		    			else
		    				cout << endl;
					}
					else{
						string_copy(int2char_buffer,0,4,data_buffer1, tmp_offset1+c_offset1[group_col_index], tmp_offset1+c_offset1[group_col_index]+4);
						int col_start_addr = char_to_int(int2char_buffer);
						int col_end_addr;
						if(c_offset1[group_col_index] == last_var_col_index1){
							string_copy(int2char_buffer,0,4,data_buffer1, tmp_offset1, tmp_offset1+4);
							int tuple_len = char_to_int(int2char_buffer);
							col_end_addr = tuple_len;
						}
						else{
							string_copy(int2char_buffer,0,4,data_buffer1,tmp_offset1 + c_offset1[group_col_index]+4, tmp_offset1 + c_offset1[group_col_index]+8);
							col_end_addr = char_to_int(int2char_buffer);
						}

						char var_group_col_value[128] = {0};
						string_copy(var_group_col_value,0,col_end_addr-col_start_addr,data_buffer1, tmp_offset1 + col_start_addr, tmp_offset1 + col_end_addr);
						cout << var_group_col_value;	
						if(agg_op.size() > 0)
		    				cout << "|";
		    			else
		    				cout << endl;
					}
				}
				else{
					if(c_type2[group_col_index] == 0){
						string_copy(int2char_buffer,0,4,data_buffer2, tmp_offset2+c_offset2[group_col_index], tmp_offset2 + c_offset2[group_col_index]+4);
						int int_group_col_value = char_to_int(int2char_buffer);
						cout << int_group_col_value;
						if(agg_op.size() > 0)
		    				cout << "|";
		    			else
		    				cout << endl;
					}
					else{
						string_copy(int2char_buffer,0,4,data_buffer2, tmp_offset2 + c_offset2[group_col_index], tmp_offset2 + c_offset2[group_col_index]+4);
						int col_start_addr = char_to_int(int2char_buffer);
						int col_end_addr;
						if(c_offset2[group_col_index] == last_var_col_index2){
							string_copy(int2char_buffer,0,4,data_buffer2, tmp_offset2, tmp_offset2+4);
							int tuple_len = char_to_int(int2char_buffer);
							col_end_addr = tuple_len;
						}
						else{
							string_copy(int2char_buffer,0,4,data_buffer2,tmp_offset2 + c_offset2[group_col_index]+4, tmp_offset2 + c_offset2[group_col_index]+8);
							col_end_addr = char_to_int(int2char_buffer);
						}

						char var_group_col_value[128] = {0};
						string_copy(var_group_col_value,0,col_end_addr-col_start_addr,data_buffer2, tmp_offset2+col_start_addr, tmp_offset2+col_end_addr);
						cout << var_group_col_value;	
						if(agg_op.size() > 0)
		    				cout << "|";
		    			else
		    				cout << endl;
					}
				}	

				vector <int> SUM,COUNT,AVG,MIN,MAX;
				for(j = 0; j < agg_op.size(); ++j){
					if(agg_tbl[j]==1){
						string_copy(int2char_buffer,0,4, data_buffer1, tmp_offset1 + c_offset1[agg_col_index[j]], tmp_offset1 + c_offset1[agg_col_index[j]]+4);
					}
					else{
						string_copy(int2char_buffer,0,4, data_buffer2, tmp_offset2 + c_offset2[agg_col_index[j]], tmp_offset2 + c_offset2[agg_col_index[j]]+4);
					}
					int agg_col_value = char_to_int(int2char_buffer);
					SUM.push_back(agg_col_value);
					COUNT.push_back(1);
					AVG.push_back((SUM[j]/COUNT[j]));
					MIN.push_back(agg_col_value); 
					MAX.push_back(agg_col_value);	  
				}			

	    		// cout << "tmp_node->key: " << tmp_node->key <<endl;
	    		// cout << "tmp_key_int: " << tmp_key_int <<endl;
	    		// cout << "tmp_type: " << tmp_type <<endl;
				if(deletekey2(tmp_key,tmp_key_int,tmp_type)){
					//cout << "successfully delete the node" << endl;
					//return;
				}


				struct NODE2 * next_equal_node = lookupkey2(tmp_key,tmp_key_int,tmp_type);
				while(next_equal_node){
					int next_equal_key_int,next_equal_page1,next_equal_offset1,next_equal_page2,next_equal_offset2,next_equal_type;

					next_equal_key_int = next_equal_node->key_int;
					next_equal_page1 = next_equal_node->page1;
					next_equal_offset1 = next_equal_node->offset1;
					next_equal_page2 = next_equal_node->page2;
					next_equal_offset2 = next_equal_node->offset2;
					next_equal_type = next_equal_node->type;

		    		// cout << "next_equal_page1: " << next_equal_page1 << endl;
		    		// cout << "next_equal_offset1: " << next_equal_offset1 << endl;
		    		// cout << "next_equal_page2: " << next_equal_page2 << endl;    		
		    		// cout << "next_equal_offset2: " << next_equal_offset2 << endl;

	    			fseek(fp[0], next_equal_page1 * PAGE_SIZE, SEEK_SET);
	    			fread(data_buffer1, sizeof(char),sizeof(data_buffer1),fp[0]);
	    			fseek(fp[1], next_equal_page2 * PAGE_SIZE, SEEK_SET);
	    			fread(data_buffer2, sizeof(char),sizeof(data_buffer2),fp[1]);  

	    			for(j = 0; j < agg_op.size(); ++j){
						if(agg_tbl[j]==1){
							string_copy(int2char_buffer,0,4, data_buffer1, next_equal_offset1 + c_offset1[agg_col_index[j]], next_equal_offset1 + c_offset1[agg_col_index[j]]+4);
						}
						else{
							string_copy(int2char_buffer,0,4, data_buffer2, next_equal_offset2 + c_offset2[agg_col_index[j]], next_equal_offset2 + c_offset2[agg_col_index[j]]+4);
						}	  		
		    			int next_equal_value = char_to_int(int2char_buffer);	
		    			// cout << "next_equal_value:   "   << next_equal_value <<endl;
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
					deletekey2(tmp_key,tmp_key_int,tmp_type);
					next_equal_node = lookupkey2(tmp_key,tmp_key_int,tmp_type);
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
						cout << "|";
					else
						cout << endl;
				}
	    	}
	    }
	}
	clean();
	clean2();
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