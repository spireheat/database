#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <limits.h>
#include "hash.cpp"
#include "hash2.cpp"
using namespace std;
#define PAGE_SIZE 4096
#define MAX_TUPLE_COUNT 197
#define TUPLE_OFFSET 800

int pos;

string getword(string, int l);
void create(string name, vector <string> c_name, vector <int> c_type);
void drop(string name);
void sel_select(vector <string> sel_name, vector <string> from_name, vector <string> w1_name, vector <string> w2_name, vector <string> w3_name, vector <string> gb_name);
void select1(vector <string> col_name, string name, string where_col, string op, string cons);
void select2(vector <string> col_name, vector <int> col_tbl, vector <string> name, vector <string> where_col, vector <int> where_tbl, vector <string> op, vector <string> cons, vector <string> join_col, vector <int> join_tbl);
void select3(string col_name, vector <string> agg_op,vector <string> agg_col_name, string name,string where_col_name, string op, string cons,string group_col_name);
void select4(string col_name, int col_tbl, vector <string> agg_op,vector <string> agg_col, vector <int> agg_tbl, vector <string> name, vector <string> where_col, vector <int> where_tbl, vector <string> op, vector <string> cons, vector <string> join_col, vector <int> join_tbl, string group_col, int group_tbl);
void analyze(string s);
void insert(string name, vector <string> value);
int cmp_keyword(string name);
int string_copy(char* , int, int, char* , int, int);
int char_to_int(char p[4]);
char* int_to_char(int n);
int file_size2(char* filename);
char a[4];

int main(int argc, char* argv[])
{
	
	ifstream ifs(argv[1]); 
 	//char buffer[256];
 	string input_buffer1;
 	string input_buffer2;
 	string output_buffer;
 	while(getline(ifs,input_buffer1)){
  		if(input_buffer1.compare(0,1,"--",0,1))
  			input_buffer2.append(input_buffer1);
 	}
 	//cout << input_buffer2<<endl;
 	int i=0,j;
 	for(i=0;i<input_buffer2.length();i++){
 		char six[7] = {input_buffer2[i+1],input_buffer2[i+2],input_buffer2[i+3],input_buffer2[i+4],input_buffer2[i+5],input_buffer2[i+6],'\0'};
 		char four[5] = {input_buffer2[i+1],input_buffer2[i+2],input_buffer2[i+3],input_buffer2[i+4],'\0'};
 		if(input_buffer2[i]!=';' && strcmp(six,"create")!=0 && strcmp(six,"select")!=0
 			&& strcmp(four,"drop")!=0 && strcmp(six,"insert")!=0){
 			if(input_buffer2[i]!='\n' || input_buffer2[i]!='\r')
 				output_buffer.push_back(input_buffer2[i]);
 		}
 		else{
 			if(input_buffer2[i]!='\n' && input_buffer2[i]!=13 && input_buffer2[i]!=32){
 				output_buffer.push_back(input_buffer2[i]);
			}
			analyze(output_buffer);
			cout << output_buffer << endl;
			output_buffer = "";
 		}
 	}


	return 1;
}

#if 0
create table tab1 (id int, name varchar); 
create table tab2 (id int, count int, text varchar); 
insert into tab1 values (1, 'first record');
insert into tab1 values (2, 'second record');
insert into tab1 values (3, 'third record');
insert into tab1 values (1, 'first again');
insert into tab2 values (1, 10, 'tab 2 data');
insert into tab2 values (3, 20, 'tab 2 data');
insert into tab2 values (5, 30, 'tab 2 data');

-- correct statement -- 
select id, count(*) from tab1 where id > 0 group by id;
select tab1.id, max(count) from tab1, tab2 where tab1.id = tab2.id and count <= 30 group by tab1.id;

-- 0. aggregation column not int -- 
select id, avg(name) from tab1 where id > 0 group by id;
-- 1. data type mismatch -- 
select id, text from tab1 group by id;
#endif


void insert(string name, vector <string> value){
    int i,j,f;
    if(cmp_keyword(name)){
        cout << "Syntax error" << endl;
        return;
    }

    char path[50];
    sprintf(path,"db/%s.tbl",name.c_str());
    FILE * fp;
    fp=fopen(path,"r");
    if(fp==NULL){   //don't have existed before
        printf("Can't insert into table %s\n",name.c_str());
        return;
    }

    fp=fopen(path,"r+");
    int sum,fix,var;
    fscanf(fp,"%d,%d,%d",&sum,&fix,&var);
    if(sum!=value.size()){
        cout << "Wrong number of columns" <<endl;
        return;
    }
    //get head information
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

    vector <int> c_length;
    vector <int> c_offset_in_tuple;

    int tuple_offset = TUPLE_OFFSET;
    int tuple_length = 4*(1+var+fix);
    int tmp_offset = 4*(1+var+fix);
    for(i=0;i<sum;i++){
        if(c_type[i] == 1){//if the column is varchar
            c_length.push_back(value[i].length()-2);
            c_offset_in_tuple.push_back(tmp_offset);
            tmp_offset += c_length[i];

            if(value[i][0] == '\'')
                tuple_length += c_length[i]; //'' 2B
            else{
                cout << "Value and column type mismatch" <<endl;
                return;
            }
        }
        else{   //if the column is int
            c_length.push_back(4);
            c_offset_in_tuple.push_back(c_offset[i]);
            if(value[i][0] == '\''){
                cout << "Value and column type mismatch" <<endl;
                return;
            }
        }
    }
    //cout << "tuple_length" << tuple_length <<endl;

    char data_buffer[4096];
    int full = 0;
    int tuple_count = 0;
    char tuple_buffer[2000] = {0};

    char *p;
    int fsize = file_size2(path);

    //cout << fsize <<endl;

    p = int_to_char(tuple_length);
    string_copy(tuple_buffer,0,4,p,0,4);
    
    //write data into the tuple buffer
    char value_buffer[200] = {0};
    for(i=0;i<sum;i++){
        if(c_type[i]==0){ //int
            value[i].copy(value_buffer,value[i].length(),0);
            p = int_to_char(atoi(value_buffer));
            string_copy(tuple_buffer,c_offset_in_tuple[i],c_offset_in_tuple[i]+4,p,0,4);
            memset(value_buffer,0,sizeof(value_buffer));
        }
        else{
            // the pointer of var column in the tuple
            p = int_to_char(c_offset_in_tuple[i]);
            string_copy(tuple_buffer,c_offset[i],c_offset[i]+4,p,0,4);
            //the value of var column in the tuple
            value[i].copy(value_buffer,value[i].length(),0);
            string_copy(tuple_buffer,c_offset_in_tuple[i],c_offset_in_tuple[i]+c_length[i],value_buffer,1,1+c_length[i]);
            memset(value_buffer,0,sizeof(value_buffer));
        }
    }

    //write into the file
    if(fsize == PAGE_SIZE){
        if(tuple_offset + tuple_length < 4000 && tuple_count < MAX_TUPLE_COUNT)
            full = 0; //not full
        else
            full = 1; //full

        p = int_to_char(full);
        string_copy(data_buffer,0,4,p,0,4);
        tuple_count = 1;
        p = int_to_char(tuple_count);
        string_copy(data_buffer,4,8,p,0,4);
        p = int_to_char(tuple_offset);
        string_copy(data_buffer,8,12,p,0,4);
        p = int_to_char(tuple_offset+tuple_length);
        string_copy(data_buffer,12,16,p,0,4);
        //write tuple into the data buffer
        string_copy(data_buffer,tuple_offset,tuple_offset+tuple_length,tuple_buffer,0,tuple_length);
        //write into the table 
        fseek(fp, fsize, SEEK_SET);
        fwrite(data_buffer,sizeof(char),sizeof(data_buffer),fp);
        return;
    }
    else{

        char int2char_buffer[4];
        fseek(fp,fsize-PAGE_SIZE, SEEK_SET);
        fscanf(fp,"%4s",int2char_buffer);
        full = char_to_int(int2char_buffer);
      	//cout << "full = " << full << endl;
        if(full){

            p = int_to_char(full);
            string_copy(data_buffer,0,4,p,0,4);
            tuple_count = 1;
            p = int_to_char(tuple_count);
            string_copy(data_buffer,4,8,p,0,4);
            p = int_to_char(tuple_offset);
            string_copy(data_buffer,8,12,p,0,4);

            p = int_to_char(tuple_offset+tuple_length);
            string_copy(data_buffer,12,16,p,0,4);
            //write tuple into the data buffer
            string_copy(data_buffer,tuple_offset,tuple_offset+tuple_length,tuple_buffer,0,tuple_length);
            //write into the table 
            fseek(fp, fsize, SEEK_SET);
            fwrite(data_buffer,sizeof(char),sizeof(data_buffer),fp);
            return;
        }

        //if not full
        fseek(fp, fsize-PAGE_SIZE, SEEK_SET);
        fread(data_buffer,sizeof(char),sizeof(data_buffer),fp);
        //tuple count
        string_copy(int2char_buffer,0,4,data_buffer,4,8);
        tuple_count = char_to_int(int2char_buffer);
        //cout << tuple_count << endl;
        //the next tuple offset
        string_copy(int2char_buffer,0,4,data_buffer, 4*(2+tuple_count), 4*(3+tuple_count) );
        tuple_offset = char_to_int(int2char_buffer);

        if(tuple_offset + tuple_length < 4000)
            full = 0; //not full
        else
            full = 1; //full

        p = int_to_char(full);
        string_copy(data_buffer,0,4,p,0,4);
        tuple_count++;
        p = int_to_char(tuple_count);
        string_copy(data_buffer,4,8,p,0,4);
 
        p = int_to_char(tuple_offset+tuple_length);
        string_copy(data_buffer,(2+tuple_count)*4,(3+tuple_count)*4,p,0,4);
        //write tuple into the data buffer
        string_copy(data_buffer,tuple_offset,tuple_offset+tuple_length,tuple_buffer,0,tuple_length); 
        //write into the table 
        fseek(fp, fsize-PAGE_SIZE, SEEK_SET);
        fwrite(data_buffer,sizeof(char),sizeof(data_buffer),fp);
        return;

    }

    return ;
}

void create(string name, vector <string> c_name, vector <int> c_type){
    int i,j;
    if(cmp_keyword(name)){
        cout << "Syntax error" << endl;
        return;
    }
    for(i=0; i < c_name.size();i++){
        if(cmp_keyword(c_name[i])){ //equal keyword
            cout << "Syntax error" << endl;
            return;
        }
    }   
    
    char path[50];
    sprintf(path,"db/%s.tbl",name.c_str());
    FILE *fp;
    fp=fopen(path,"r");
 
    if(fp!=NULL){   //have existed before
        printf("Can't create table %s\n",name.c_str());
    }
    else{
        fp=fopen(path,"w");
        char head_buffer[4096] = {0};
        int sum_col = c_type.size();
        int fix_col = 0;
        int var_col = 0;
        int last_var_col_index = -1;
        vector <int> offset;
        for(j=0;j<c_type.size();j++){
            if(c_type[j] == 0)
                fix_col++;
            else{
                var_col++;
                last_var_col_index = j;
            }
        }

        int fix_offset_addr = 4*(1+var_col);
        int var_offset_addr = 4;
        for(j=0;j<c_type.size();j++){
            if(c_type[j] == 0){
                offset.push_back(fix_offset_addr);
                fix_offset_addr += 4;
            }
            else{
                offset.push_back(var_offset_addr);
                var_offset_addr += 4;
            }
        }
        char buffer[46];
        sprintf(buffer,"%d,%d,%d,%d",sum_col,fix_col,var_col,last_var_col_index);
        
        for(i=0;i<46;i++)
            head_buffer[i] = buffer[i];
        int start_addr = 46;
        for(j=0;j<c_type.size();j++){
            char name_buffer[128] = {0};
            char type_buffer[22] = {0};
            strcpy(name_buffer,c_name[j].c_str());
            int type = c_type[j];
            sprintf(type_buffer,"%d,%d",type,offset[j]);
            for(i=0;i<128;i++)
                head_buffer[start_addr++] = name_buffer[i];
            for(i=0;i<22;i++)
                head_buffer[start_addr++] = type_buffer[i];
        }
        fwrite(head_buffer,sizeof(char),sizeof(head_buffer),fp);
        printf("Successfully created table %s\n",name.c_str());
        fclose(fp);
    }
}

void drop(string name){
    int i,j,f;
    if(cmp_keyword(name)){
        cout << "Syntax error" << endl;
        return;
    }

    char path[50];
    sprintf(path,"db/%s.tbl",name.c_str());
    FILE * fp;
    fp=fopen(path,"r");
    
    if(fp!=NULL){   //have existed before
        remove(path);
        fclose(fp);
   		printf("Successfully dropped table %s\n",name.c_str());
    	return;
    }
    else{
        printf("Can't drop table %s\n",name.c_str());
        return;
    }
    return;
}

void analyze(string s)
{
	pos = 0;
	string tmp;

	tmp = getword(s, pos);

	if(tmp == "create") //create
	{
		string next;
		string type;

		string tname;
		vector <string> c_name;
		vector <int> c_type;
		
		if(s.find(";", pos) == -1)
		{
			cout << "Syntax error" << endl;
			return;
		}

		tmp = getword(s, pos);
		if(tmp != "table")
		{
			cout << "Syntax error" << endl;
			return;
		}
		else
		{
			tname = getword(s, pos);
			tmp = getword(s, pos);

			if(tmp != "(")
			{
				cout << "Syntax error" << endl;
				return;
			}
			else
			{
				next = getword(s, pos);
				if(next == ")" || next == "," || next == ";")
				{
					cout << "Syntax error" << endl;
					return;
				}
				else
				{
					while(next != ")")
					{
						c_name.push_back(next);
						type = getword(s, pos);
						if(type == "int")
						{
							c_type.push_back(0);
						}
						else if(type == "varchar")
						{
							c_type.push_back(1);
						}
						else
						{
							cout << "Syntax error" << endl;
							return;
						}
						next = getword(s, pos);
						if(next == ",")
						{
							next = getword(s, pos);
							if(next == ")")
							{
								cout << "Syntax error" << endl;
								return;
							}
							else
								;
						}
						else if(next == ")")
							;
						else
						{
							cout << "Syntax error" << endl;
							return;
						}
					}
				}
			}
		}
		create(tname,c_name,c_type);
		pos = 0;
		c_name.clear();
		c_type.clear();
	}
	else if(tmp == "drop") //drop
	{
		string tname;

		if(s.find(";", pos) == -1)
		{
			cout << "Syntax error" << endl;
			return;
		}

		tmp = getword(s, pos);
		if(tmp != "table")
		{
			cout << "Syntax error" << endl;
			return;
		}
		else
		{
			tname = getword(s, pos);
			tmp = getword(s, pos);
			if(tmp != ";")
			{
				cout << "Syntax error" << endl;
				return;
			}

		}
		drop(tname);
		pos = 0;
	}
	else if(tmp == "insert") //insert
	{
		string next;
		
		string tname;
		vector <string> c_name;

		if(s.find(";", pos) == -1)
		{
			cout << "Syntax error" << endl;
			return;
		}

		tmp = getword(s, pos);
		if(tmp != "into")
		{
			cout << "Syntax error1" << endl;
			return;
		}
		else
		{
			tname = getword(s, pos);
			next = getword(s, pos);
			if(next != "values")
			{
				cout << "Syntax error2" << endl;
				return;
			}
			else
			{
				next = getword(s, pos);
				if(next != "(")
				{
					cout << "Syntax error3" << endl;
					return;
				}
				else
				{
					next = getword(s, pos);
					if(next == ")" || next == "," || next == ";")
					{
						cout << "Syntax error4" << endl;
						return;
					}
					else
					{
						while(next != ")")
						{
							c_name.push_back(next);

							next = getword(s, pos);
							if(next == ",")
							{
								next = getword(s, pos);
								if(next == ")")
								{
									cout << "Syntax error5" << endl;
									return;
								}
								else
									;
							}
							else if(next == ")")
								;
							else
							{
								cout << "Syntax error6" << endl;
								return;
							}
						}
					}
				}
			}
		}
		insert(tname, c_name);
		pos = 0;
		c_name.clear();
	}
	else if(tmp == "select") //select
	{
		vector <string> sel_name;
		vector <string> from_name;
		vector <string> w1_name;
		vector <string> w2_name;
		vector <string> w3_name;
		vector <string> gb_name;
		string next;
		string t1, t2, t3;
		
		if(s.find(";", pos) == -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		next = getword(s, pos);

		if(next == "*")
		{
			if(getword(s, pos) != "from")
			{
				cout << "Syntax error1" << endl;
				return;
			}
			sel_name.push_back(next);
		}
		else if(next == "from")
		{
			cout << "Syntax error2" << endl;
			return;
		}
		else
		{
			while(next != "from")
			{
				if(next == "count")
				{
					t1 = getword(s, pos);
					if(t1 != "(")
					{
						cout << "Syntax error3" << endl;
						return;
					}
					else
					{
						t2 = getword(s, pos);
						t3 = getword(s, pos);
						if(t3 != ")")
						{
							cout << "Syntax error4" << endl;
							return;
						}
						sel_name.push_back(next + t1 + t2 + t3);
					}
				}
				else if(next == "sum" || next == "avg" || next == "min" || next == "max")
				{
					t1 = getword(s, pos);
					if(t1 != "(")
					{
						cout << "Syntax error5" << endl;
						return;
					}
					else
					{
						t2 = getword(s, pos);
						if(t2 == "*")
						{
							cout << "Syntax error6" << endl;
							return;
						}
						t3 = getword(s, pos);
						if(t3 != ")")
						{
							cout << "Syntax error7" << endl;
							return;
						}
						sel_name.push_back(next + t1 + t2 + t3);
					}
				}
				else //normal column
				{
					sel_name.push_back(next);
				}
				t1 = getword(s, pos);
				if(t1 == "from")
				{
					next = t1;
				}
				else if(t1 == ",")
				{
					next = getword(s, pos);
					if(next == "from")
					{
						cout << "Syntax error8" << endl;
						return;
					}
				}
				else
				{
					cout << "Syntax error9" << endl;
					return;
				}
			}
		}//end before from
		//begin from
		next = getword(s, pos);
		if(next == "(" || next == ")" || next == "," || next == ";" || next == "where" || next == "group")
		{
			cout << "Syntax error10" << endl;
			return;
		}
		else
		{
			while(next != ";" && next != "where" && next != "group")
			{
				from_name.push_back(next);//table = next
				t1 = getword(s, pos);
				if(t1 == ";" || t1 == "where" || t1 == "group")
				{
					next = t1;
				}
				else if(t1 == ",")
				{
					next = getword(s, pos);
					if(next == ";" || next == "where" || next == "group")
					{
						cout << "Syntax error11" << endl;
						return;
					}
				}
				else
				{
					cout << "Syntax error" << endl;
					return;
				}
			}
		}//end after from
		if(next == "where")
		{
			next = getword(s, pos);
			w1_name.push_back(next);
			t1 = getword(s, pos);
			if(t1 != "<" && t1 != "<=" && t1 != ">" && t1 != ">=" && t1 != "=" && t1 != "!=" && t1 != "like" && t1 != "not")
			{
				cout << "Syntax error12" << endl;
				return;
			}
			if(t1 == "not")
			{
				if(getword(s, pos) != "like")
				{
					cout << "Syntax error13" << endl;
					return;
				}
			}
			w2_name.push_back(t1);
			t2 = getword(s, pos);
			w3_name.push_back(t2);

			next = getword(s, pos);
			if(next == "group" || next == ";")
				;
			else if(next == "and")
			{
				next = getword(s, pos);
				w1_name.push_back(next);
				t1 = getword(s, pos);
				if(t1 != "<" && t1 != "<=" && t1 != ">" && t1 != ">=" && t1 != "=" && t1 != "!=" && t1 != "like" && t1 != "not")
				{
					cout << "Syntax error14" << endl;
					return;
				}
				if(t1 == "not")
				{
					if(getword(s, pos) != "like")
					{
						cout << "Syntax error15" << endl;
						return;
					}
				}
				w2_name.push_back(t1);
				t2 = getword(s, pos);
				w3_name.push_back(t2);

				next = getword(s, pos);
				if(next == "group" || next == ";")
					;
				else if(next == "and")
				{
					next = getword(s, pos);
					w1_name.push_back(next);
					t1 = getword(s, pos);
					if(t1 != "<" && t1 != "<=" && t1 != ">" && t1 != ">=" && t1 != "=" && t1 != "!=" && t1 != "like" && t1 != "not")
					{
						cout << "Syntax error16" << endl;
						return;
					}
					if(t1 == "not")
					{
						if(getword(s, pos) != "like")
						{
							cout << "Syntax error17" << endl;
							return;
						}
					}
					w2_name.push_back(t1);
					t2 = getword(s, pos);
					w3_name.push_back(t2);

					next = getword(s, pos);
					if(next != "group" && next != ";")
					{
						cout << "Syntax error18" << endl;
						return;
					}
				}
				else
				{
					cout << "Syntax error19" << endl;
					return;
				}
			}
			else
			{
				cout << "Syntax error20" << endl;
				return;
			}
		}
		if(next == "group")
		{
			next = getword(s, pos);
			if(next != "by")
			{
				cout << "Syntax error21" << endl;
				return;
			}
			else
			{
				t1 = getword(s, pos);
				if(t1 == ";")
				{
					cout << "Syntax error22" << endl;
					return;
				}
				else
				{
					if(getword(s, pos) != ";")
					{
						cout << "Syntax error23" << endl;
						return;
					}
					gb_name.push_back(t1);
				}
			}
		}
		if(next == ";")
		{
		}
		sel_select(sel_name, from_name, w1_name, w2_name, w3_name, gb_name);
		pos = 0;
		sel_name.clear();
		from_name.clear();
		w1_name.clear();
		w2_name.clear();
		w3_name.clear();
		gb_name.clear();
	}
	return;
}
#if 1
void sel_select(vector <string> sel_name, vector <string> from_name, vector <string> w1_name, vector <string> w2_name, vector <string> w3_name, vector <string> gb_name)
{
	if(from_name.size() == 1 && gb_name.size() == 0) //select1
	{
		vector <string> agg_op;
		vector <string> agg_col;
		vector <string> col_name;
		int i;
		for(i = 0; i < sel_name.size(); i++)
		{
			if(sel_name[i].find("(") != -1)
			{
				int left, right, dot;
				left = sel_name[i].find("(");
				right = sel_name[i].find(")");
				dot = sel_name[i].find(".");
				if(dot != -1)
				{
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(dot + 1, right - dot - 1));
				}
				else
				{
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(left + 1, right - left - 1));
				}
			}
			else if(sel_name[i].find(".") != -1)
			{
				int dot;
				string tmp;
				dot = sel_name[i].find(".");
				tmp = sel_name[i].substr(0, dot);
				if(tmp != from_name[0])
				{
					cout << "Table " << tmp << " doesn't exist" << endl;
					return;
				}
				col_name.push_back(sel_name[i].substr(dot + 1, sel_name[i].size()- dot -1));
			}
			else
			{
				col_name.push_back(sel_name[i]);
			}
		}

		if(w1_name.size() == 0)
			select1(col_name, from_name[0], "", "", "");
		else
			select1(col_name, from_name[0], w1_name[0], w2_name[0], w3_name[0]);
		
		agg_op.clear();
		agg_col.clear();
		col_name.clear();
	}
	else if(from_name.size() > 1 && gb_name.size() == 0) //select2
	{
		vector <string> agg_op;
		vector <string> agg_col;
		vector <string> col_name;
		vector <int>	col_tbl;
		vector <string> where_col;
		vector <int> 	where_tbl;
		vector <string> op;
		vector <string> cons;
		vector <string> join_col;
		vector <int> 	join_tbl;
		int i, j, k;
		for(i = 0; i < sel_name.size(); i++)
		{
			if(sel_name[i].find("(") != -1)
			{
				int left, right, dot;
				left = sel_name[i].find("(");
				right = sel_name[i].find(")");
				dot = sel_name[i].find(".");
				if(dot != -1)
				{
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(dot + 1, right - dot - 1));
				}
				else
				{
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(left + 1, right - left - 1));
				}
			}
			else if(sel_name[i].find(".") != -1)
			{
				int dot;
				string tmp;
				dot = sel_name[i].find(".");
				tmp = sel_name[i].substr(0, dot);
				for(j = 0; j < from_name.size(); j++)
				{
					if(tmp == from_name[j])
					{
						col_tbl.push_back(j+1);
						break;
					}
				}
				if(j == from_name.size())
				{
					cout << "Table " << tmp << " doesn't exist" << endl;
					return;
				}
				col_name.push_back(sel_name[i].substr(dot + 1, sel_name[i].size()- dot -1));
			}
			else
			{
				col_name.push_back(sel_name[i]);
				col_tbl.push_back(0);
			}
		}
		vector <int> w1_type;
		vector <int> w3_type;
		int count = 0;
		for(i = 0; i < w1_name.size(); i++)
		{
			w1_type.push_back(0);
			for(j = 0; j < w1_name[i].length(); j++)
			{
				if(w1_name[i][0] == '\'')
				{
					w1_type[i] = 1;
					break;
				}
				if(w1_name[i][j] < '0' || w1_name[i][j] > '9')
				{
					w1_type[i] = 2;
					break;
				}
			}
			if(w1_type[i] != 2)
			{
				cout << "Syntax error" << endl;
				return;
			}
		}
		for(i = 0; i < w3_name.size(); i++)
		{
			w3_type.push_back(0);
			for(j = 0; j < w3_name[i].length(); j++)
			{
				if(w3_name[i][0] == '\'')
				{
					w3_type[i] = 1;
					break;
				}
				if(w3_name[i][j] < '0' || w3_name[i][j] > '9')
				{
					w3_type[i] = 2;
					break;
				}
			}
		}
		for(i = 0; i < w1_name.size(); i++)
		{
			if(w1_type[i] == 2 && w3_type[i] == 2)
			{
				count++;
				string tmp;
				tmp = w1_name[0];
				w1_name[0] = w1_name[i];
				w1_name[i] = tmp;
				
				tmp = w3_name[0];
				w3_name[0] = w3_name[i];
				w3_name[i] = tmp;
			}
		}
		if(count != 1)
		{
			cout << "Join predicate error" << endl;
		}
		//
		for(i = 1; i < w1_name.size(); i++)
		{
			if(w1_name[i].find(".") != -1)
			{
				int dot;
				string tmp;
				dot = w1_name[i].find(".");
				tmp = w1_name[i].substr(0, dot);
				for(j = 0; j < from_name.size(); j++)
				{
					if(tmp == from_name[j])
					{
						where_tbl.push_back(j+1);
						break;
					}
				}
				if(j == from_name.size())
				{
					cout << "Table " << tmp << " doesn't exist" << endl;
					return;
				}
				where_col.push_back(w1_name[i].substr(dot + 1, w1_name[i].size()- dot -1));
				op.push_back(w2_name[i]);
				cons.push_back(w3_name[i]);
			}
			else
			{
				where_col.push_back(w1_name[i]);
				where_tbl.push_back(0);
				op.push_back(w2_name[i]);
				cons.push_back(w3_name[i]);
			}
		}
		//
		if(w1_name[0].find(".") != -1)
		{
			int dot;
			string tmp;
			dot = w1_name[0].find(".");
			tmp = w1_name[0].substr(0, dot);
			for(j = 0; j < from_name.size(); j++)
			{
				if(tmp == from_name[j])
				{
					join_tbl.push_back(j+1);
					break;
				}
			}
			if(j == from_name.size())
			{
				cout << "Table " << tmp << " doesn't exist" << endl;
				return;
			}
			join_col.push_back(w1_name[0].substr(dot + 1, w1_name[0].size()- dot -1));
		}
		else
		{
			join_col.push_back(w1_name[0]);
			join_tbl.push_back(0);
		}
		if(w3_name[0].find(".") != -1)
		{
			int dot;
			string tmp;
			dot = w3_name[0].find(".");
			tmp = w3_name[0].substr(0, dot);
			for(j = 0; j < from_name.size(); j++)
			{
				if(tmp == from_name[j])
				{
					join_tbl.push_back(j+1);
					break;
				}
			}
			if(j == from_name.size())
			{
				cout << "Table " << tmp << " doesn't exist" << endl;
				return;
			}
			join_col.push_back(w3_name[0].substr(dot + 1, w3_name[0].size()- dot -1));
		}
		else
		{
			join_col.push_back(w3_name[0]);
			join_tbl.push_back(0);
		}
		
		select2(col_name, col_tbl, from_name, where_col, where_tbl, op, cons, join_col, join_tbl);
		
		agg_op.clear();
		agg_col.clear();
		col_name.clear();
		col_tbl.clear();
		where_col.clear();
		where_tbl.clear();
		op.clear();
		cons.clear();
		join_col.clear();
		join_tbl.clear();
		w1_type.clear();
		w3_type.clear();
	}
	else if(from_name.size() == 1 && gb_name.size() == 1) //select3
	{
		vector <string> agg_op;
		vector <string> agg_col;
		vector <string> col_name;
		string group_col_name;
		int i;
		for(i = 0; i < sel_name.size(); i++)
		{
			if(sel_name[i].find("(") != -1)
			{
				int left, right, dot;
				left = sel_name[i].find("(");
				right = sel_name[i].find(")");
				dot = sel_name[i].find(".");
				if(dot != -1)
				{
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(dot + 1, right - dot - 1));
				}
				else
				{
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(left + 1, right - left - 1));
				}
			}
			else if(sel_name[i].find(".") != -1)
			{
				int dot;
				string tmp;
				dot = sel_name[i].find(".");
				tmp = sel_name[i].substr(0, dot);
				if(tmp != from_name[0])
				{
					cout << "Table " << tmp << " doesn't exist" << endl;
					return;
				}
				col_name.push_back(sel_name[i].substr(dot + 1, sel_name[i].size()- dot -1));
			}
			else
			{
				col_name.push_back(sel_name[i]);
			}
		}

		if(col_name.size() != 1)
		{
			cout << "Non-group_by column " << col_name[1] << " in select list" << endl;
			return;
		}
		
		if(gb_name[0].find(".") != -1)
		{
			int dot;
			string tmp;
			dot = sel_name[i].find(".");
			tmp = sel_name[i].substr(0, dot);
			if(tmp != from_name[0])
			{
				cout << "Table " << tmp << " doesn't exist" << endl;
				return;
			}
			group_col_name = gb_name[0].substr(dot + 1, gb_name[0].size()- dot -1);
		}
		else
		{
			group_col_name = gb_name[0];
		}
		
		if(w1_name.size() == 0)
			select3(col_name[0], agg_op, agg_col, from_name[0], "", "", "", group_col_name);
		else
			select3(col_name[0], agg_op, agg_col, from_name[0], w1_name[0], w2_name[0], w3_name[0], group_col_name);
		
		agg_op.clear();
		agg_col.clear();
		col_name.clear();
	}
	else //select4
	{
		vector <string> agg_op;
		vector <string> agg_col;
		vector <int>	agg_tbl;
		vector <string> col_name;
		vector <int>	col_tbl;
		vector <string> where_col;
		vector <int> 	where_tbl;
		vector <string> op;
		vector <string> cons;
		vector <string> join_col;
		vector <int> 	join_tbl;
		string group_col_name;
		int group_tbl = 0;
		int i, j, k;
		for(i = 0; i < sel_name.size(); i++)
		{
			if(sel_name[i].find("(") != -1)
			{
				int left, right, dot;
				string agg_t;
				left = sel_name[i].find("(");
				right = sel_name[i].find(")");
				dot = sel_name[i].find(".");
				if(dot != -1)
				{
					agg_t = sel_name[i].substr(left+1, dot - left - 1);
					if(agg_t == from_name[0])
						agg_tbl.push_back(1);
					else
						agg_tbl.push_back(2);
						
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(dot + 1, right - dot - 1));
				}
				else
				{
					agg_tbl.push_back(0);
					agg_op.push_back(sel_name[i].substr(0, left));
					agg_col.push_back(sel_name[i].substr(left + 1, right - left - 1));
				}
			}
			else if(sel_name[i].find(".") != -1)
			{
				int dot;
				string tmp;
				dot = sel_name[i].find(".");
				tmp = sel_name[i].substr(0, dot);
				for(j = 0; j < from_name.size(); j++)
				{
					if(tmp == from_name[j])
					{
						col_tbl.push_back(j+1);
						break;
					}
				}
				if(j == from_name.size())
				{
					cout << "Table " << tmp << " doesn't exist" << endl;
					return;
				}
				col_name.push_back(sel_name[i].substr(dot + 1, sel_name[i].size()- dot -1));
			}
			else
			{
				col_name.push_back(sel_name[i]);
				col_tbl.push_back(0);
			}
		}
		vector <int> w1_type;
		vector <int> w3_type;
		int count = 0;
		for(i = 0; i < w1_name.size(); i++)
		{
			w1_type.push_back(0);
			for(j = 0; j < w1_name[i].length(); j++)
			{
				if(w1_name[i][0] == '\'')
				{
					w1_type[i] = 1;
					break;
				}
				if(w1_name[i][j] < '0' || w1_name[i][j] > '9')
				{
					w1_type[i] = 2;
					break;
				}
			}
			if(w1_type[i] != 2)
			{
				cout << "Syntax error" << endl;
				return;
			}
		}
		for(i = 0; i < w3_name.size(); i++)
		{
			w3_type.push_back(0);
			for(j = 0; j < w3_name[i].length(); j++)
			{
				if(w3_name[i][0] == '\'')
				{
					w3_type[i] = 1;
					break;
				}
				if(w3_name[i][j] < '0' || w3_name[i][j] > '9')
				{
					w3_type[i] = 2;
					break;
				}
			}
		}
		for(i = 0; i < w1_name.size(); i++)
		{
			if(w1_type[i] == 2 && w3_type[i] == 2)
			{
				count++;
				string tmp;
				tmp = w1_name[0];
				w1_name[0] = w1_name[i];
				w1_name[i] = tmp;
				
				tmp = w3_name[0];
				w3_name[0] = w3_name[i];
				w3_name[i] = tmp;
			}
		}
		if(count != 1)
		{
			cout << "Join predicate error" << endl;
		}
		//
		for(i = 1; i < w1_name.size(); i++)
		{
			if(w1_name[i].find(".") != -1)
			{
				int dot;
				string tmp;
				dot = w1_name[i].find(".");
				tmp = w1_name[i].substr(0, dot);
				for(j = 0; j < from_name.size(); j++)
				{
					if(tmp == from_name[j])
					{
						where_tbl.push_back(j+1);
						break;
					}
				}
				if(j == from_name.size())
				{
					cout << "Table " << tmp << " doesn't exist" << endl;
					return;
				}
				where_col.push_back(w1_name[i].substr(dot + 1, w1_name[i].size()- dot -1));
				op.push_back(w2_name[i]);
				cons.push_back(w3_name[i]);
			}
			else
			{
				where_col.push_back(w1_name[i]);
				where_tbl.push_back(0);
				op.push_back(w2_name[i]);
				cons.push_back(w3_name[i]);
			}
		}
		//
		if(w1_name[0].find(".") != -1)
		{
			int dot;
			string tmp;
			dot = w1_name[0].find(".");
			tmp = w1_name[0].substr(0, dot);
			for(j = 0; j < from_name.size(); j++)
			{
				if(tmp == from_name[j])
				{
					join_tbl.push_back(j+1);
					break;
				}
			}
			if(j == from_name.size())
			{
				cout << "Table " << tmp << " doesn't exist" << endl;
				return;
			}
			join_col.push_back(w1_name[0].substr(dot + 1, w1_name[0].size()- dot -1));
		}
		else
		{
			join_col.push_back(w1_name[0]);
			join_tbl.push_back(0);
		}
		if(w3_name[0].find(".") != -1)
		{
			int dot;
			string tmp;
			dot = w3_name[0].find(".");
			tmp = w3_name[0].substr(0, dot);
			for(j = 0; j < from_name.size(); j++)
			{
				if(tmp == from_name[j])
				{
					join_tbl.push_back(j+1);
					break;
				}
			}
			if(j == from_name.size())
			{
				cout << "Table " << tmp << " doesn't exist" << endl;
				return;
			}
			join_col.push_back(w3_name[0].substr(dot + 1, w3_name[0].size()- dot -1));
		}
		else
		{
			join_col.push_back(w3_name[0]);
			join_tbl.push_back(0);
		}

		if(gb_name[0].find(".") != -1)
		{
			int dot;
			string tmp;
			dot = gb_name[0].find(".");
			tmp = gb_name[0].substr(0, dot);
			if(tmp == from_name[0])
				group_tbl = 1;
			else
				group_tbl = 2;
	
			group_col_name = gb_name[0].substr(dot + 1, gb_name[0].size()- dot -1);
		}
		else
		{
			group_tbl = 0;
			group_col_name = gb_name[0];
		}
		
		select4(col_name[0], col_tbl[0], agg_op, agg_col, agg_tbl, from_name, where_col, where_tbl, op, cons, join_col, join_tbl, group_col_name, group_tbl);
		
		agg_op.clear();
		agg_col.clear();
		col_name.clear();
		col_tbl.clear();
		where_col.clear();
		where_tbl.clear();
		op.clear();
		cons.clear();
		join_col.clear();
		join_tbl.clear();
		w1_type.clear();
		w3_type.clear();
	}
		
}
#endif
string getword(string s, int l)
{
	int i, j;
	string tmp;
	for(i = l; i < s.length(); i++)
	{
		if(s[i] == ' ' || s[i] == '\r')
			pos++;
		else if(s[i] == '(')
		{
			pos++;
			return "(";
		}
		else if(s[i] == ')')
		{
			pos++;
			return ")";
		}
		else if(s[i] == ';')
		{
			pos++;
			return ";";
		}
		else if(s[i] == ',')
		{
			pos++;
			return ",";
		}
		else
			break;
	}

	if(s[i] == '\'')
	{
		tmp += '\'';
		for(j = i+1; j < s.length(); j++)
		{
			if(s[j] != '\'')
				tmp += s[j];
			else
			{
				tmp += s[j];
				j++;
				break;
			}
		}
	}
	else
	{
		for(j = i; j < s.length(); j++)
		{
			if(s[j] != ' ' && s[j] != '(' && s[j] != ')' && s[j] != ';' && s[j] != ',')
				tmp += s[j];
			else
				break;
		}
	}
	pos = j;
	return tmp;
}

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

void select2(vector <string> col_name, vector <int> col_tbl, vector <string> name, vector <string> where_col, vector <int> where_tbl, vector <string> op, vector <string> cons, vector <string> join_col, vector <int> join_tbl){
	FILE * fp[2];
	int i, j;

	for(i = 0; i < col_name.size(); i++)
	{
		if(cmp_keyword(col_name[i]))
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
		for(i = 0; i < col_name.size(); i++)
		{
			if(col_tbl[i] == 1)
				col_tbl[i] = 2;
			else if(col_tbl[i] == 2)
				col_tbl[i] = 1;
		}
		for(i = 0; i < where_col.size(); i++)
		{
			if(where_tbl[i] == 1)
				where_tbl[i] = 2;
			else if(where_tbl[i] == 2)
				where_tbl[i] = 1;
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

	//if the column exists
	vector <int> col_index;
    for(j=0;j<col_name.size();j++){
    	col_index.push_back(-1);
		//cout << "col_tbl[j]: " << col_tbl[j] <<endl;
		if(col_tbl[j] == 0)
		{
			for(i=0;i<sum1;i++){
				//cout << "c_name1[i]" << c_name1[i] <<endl;
				//cout << "col_name[j]" << col_name[j] <<endl;

	    		if(c_name1[i] == col_name[j])
	    			col_index[j] = i;
			}
			//cout << "col_index[j]:  " << col_index[j] <<endl;
			if(col_name[j] == "*")
			{
				;
			}
			else if(col_index[j] == -1){
				for(i=0;i<sum2;i++){
					if(c_name2[i] == col_name[j])
					{
						col_index[j] = i;
						col_tbl[j] = 2;
					}
				}
				if(col_index[j] == -1 && col_name[j] != "*"){
					printf("Column %s doesn't exist\n",col_name[j].c_str());
					return;
				}
			}
			else
			{
				int tmp = -1;
				for(i=0;i<sum2;i++){
					if(c_name2[i] == col_name[j])
						tmp = i;
				}
				if(tmp == -1){
					col_tbl[j] = 1;
				}
				else
				{
					cout << "Ambiguous column " << col_name[j] << endl;
					return;
				}
			}
		}
		else if(col_tbl[j] == 1)
		{
			for(i=0;i<sum1;i++){
    		if(c_name1[i] == col_name[j])
    			col_index[j] = i;
			}
			if(col_index[j] == -1 && col_name[j] != "*"){
				printf("Column %s doesn't exist\n",col_name[j].c_str());
				return;
			}
		}
		else
		{
			for(i=0;i<sum2;i++){
				if(c_name2[i] == col_name[j])
					col_index[j] = i;
			}
			if(col_index[j] == -1 && col_name[j] != "*"){
				printf("Column %s doesn't exist\n",col_name[j].c_str());
				return;
			}
		}
    }


	//column and op mismatch error
	vector <int> where_col_index;
	for(i = 0; i < where_col.size(); i++)
		where_col_index.push_back(-1);
	//int join_num = 0;
	//int join_index = -1;

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

	//Join predicate error
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
			else
			{
				int tmp = -1;
				for(i=0;i<sum2;i++){
					if(c_name2[i] == join_col[j])
						tmp = i;
				}
				if(tmp == -1){
					join_tbl[j] = 1;
				}
				else
				{
					cout << "Ambiguous column " << join_col[j] << endl;
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
    if(col_name[0]=="*"){
	    for(j = 0; j < c_name1.size(); j++){
	    	cout << c_name1[j] << "|";
	    }
		for(j = 0; j < c_name2.size(); j++){
			if( j < c_name2.size() -1)
				cout << c_name2[j] << "|";
			else
				cout << c_name2[j] << endl;
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
					if(col_name[0] == "*"){
						//print all the column in the tuple1
		    			for(k = 0; k < c_name1.size(); k++){
		    				if(c_type1[k] == 0){
								string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1 + c_offset1[k], tuple_offset1+c_offset1[k]+4);
								int int_col_value = char_to_int(int2char_buffer);
								cout << int_col_value << "|";
							}
							else{
								string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1 + c_offset1[k], tuple_offset1+c_offset1[k]+4);
								int col_start_addr = char_to_int(int2char_buffer);
								int col_end_addr;
								string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1, tuple_offset1+4);
								int tuple_len1 = char_to_int(int2char_buffer);

								if(k == last_var_col_index1){
									col_end_addr = tuple_len1;
								}
								else{
									string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1+c_offset1[k]+4, tuple_offset1+c_offset1[k]+8);
									col_end_addr = char_to_int(int2char_buffer);
								}
								// cout << "col_start_addr" << col_start_addr <<endl;
								// cout << "col_end_addr" << col_end_addr <<endl;

								char var_col_value[128] = {0};
								string_copy(var_col_value,0,col_end_addr-col_start_addr,data_buffer1, tuple_offset1+col_start_addr, tuple_offset1+col_end_addr);
									cout << var_col_value << "|";		
							}
						}
						//print all the column in the tuple2
		    			for(k = 0; k < c_name2.size(); k++){
		    				if(c_type2[k] == 0){
								string_copy(int2char_buffer,0,4,data_buffer2, tuple_offset2 + c_offset2[k], tuple_offset2+c_offset2[k]+4);
								int int_col_value = char_to_int(int2char_buffer);
								if(k < c_name2.size()-1)
									cout << int_col_value << "|";
								else
									cout << int_col_value << endl;
							}
							else{
								string_copy(int2char_buffer,0,4,data_buffer2, tuple_offset2 + c_offset2[k], tuple_offset2+c_offset2[k]+4);
								int col_start_addr = char_to_int(int2char_buffer);
								int col_end_addr;
								if(k == last_var_col_index2){
									col_end_addr = tuple_len2;
								}
								else{
									string_copy(int2char_buffer,0,4,data_buffer2, tuple_offset2+c_offset2[k]+4, tuple_offset2+c_offset2[k]+8);
									col_end_addr = char_to_int(int2char_buffer);
								}

								char var_col_value[128] = {0};
								string_copy(var_col_value,0,col_end_addr-col_start_addr,data_buffer2, tuple_offset2+col_start_addr, tuple_offset2+col_end_addr);
					    		if(k < c_name2.size()-1)
									cout << var_col_value << "|";
								else
									cout << var_col_value << endl;			
							}
						}
					}
					else{ 	// not select *
						for(k = 0; k < col_tbl.size(); k++)
						{		
							// cout << "THis is the kth col: "<< k <<endl;
							// cout << "This is table " << col_tbl[k] << " column" <<endl;
							if(col_tbl[k] == 1)
							{
								if(c_type1[col_index[k]] == 0){
									string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1+c_offset1[col_index[k]], tuple_offset1+c_offset1[col_index[k]]+4);
									int int_col_value = char_to_int(int2char_buffer);
									if(k < col_index.size()-1)
										cout << int_col_value << "|";
									else
										cout << int_col_value << endl;
								}
								else{
									string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1 + c_offset1[col_index[k]], tuple_offset1+c_offset1[col_index[k]]+4);
									int col_start_addr = char_to_int(int2char_buffer);
									int col_end_addr;
									string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1, tuple_offset1+4);
									int tuple_len1 = char_to_int(int2char_buffer);
									if(col_index[k] == last_var_col_index1){
										col_end_addr = tuple_len1;
									}
									else{
										string_copy(int2char_buffer,0,4,data_buffer1, tuple_offset1+c_offset1[col_index[k]]+4, tuple_offset1+c_offset1[col_index[k]]+8);
										col_end_addr = char_to_int(int2char_buffer);
									}
									// cout << "col_start_addr" << col_start_addr <<endl;
									// cout << "col_end_addr" << col_end_addr <<endl;

									char var_col_value[128] = {0};
									string_copy(var_col_value,0,col_end_addr-col_start_addr,data_buffer1, tuple_offset1+col_start_addr, tuple_offset1+col_end_addr);
						    		if(k < col_index.size()-1)
										cout << var_col_value << "|";
									else
										cout << var_col_value << endl;			
								}
							}
							else
							{
								if(c_type2[col_index[k]] == 0){
									string_copy(int2char_buffer,0,4,data_buffer2, tuple_offset2+c_offset2[col_index[k]], tuple_offset2+c_offset2[col_index[k]]+4);
									int int_col_value = char_to_int(int2char_buffer);
									if(k < col_index.size()-1)
										cout << int_col_value << "|";
									else
										cout << int_col_value << endl;
								}
								else{
									// cout << "hhhh" <<endl;
									// cout << "col_index[k]:  " << col_index[k]  <<endl;
									// cout << "last_var_col_index2:  " << last_var_col_index2  <<endl;
									string_copy(int2char_buffer,0,4,data_buffer2, tuple_offset2+c_offset2[col_index[k]], tuple_offset2+c_offset2[col_index[k]]+4);
									int col_start_addr = char_to_int(int2char_buffer);
									int col_end_addr;
									if(col_index[k] == last_var_col_index2){
										col_end_addr = tuple_len2;
									}
									else{
										string_copy(int2char_buffer,0,4,data_buffer2, tuple_offset2+c_offset2[col_index[k]]+4, tuple_offset2+c_offset2[col_index[k]]+8);
										col_end_addr = char_to_int(int2char_buffer);
									}

									char var_col_value[128] = {0};
									string_copy(var_col_value,0,col_end_addr-col_start_addr,data_buffer2, tuple_offset2+col_start_addr, tuple_offset2+col_end_addr);
						    		if(k < col_index.size()-1)
										cout << var_col_value << "|";
									else
										cout << var_col_value << endl;			
								}
							}
						}
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
	clean();
    return;
}

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
	if(agg_op.size() > 0)
    	cout << col_name << "|";
    else
    	cout << col_name << endl;
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
		MIN.push_back(INT_MIN); 
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
			if(col_index == -1){
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
		if(col_index == -1){
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
		if(col_index == -1){
			printf("Column %s doesn't exist\n",col_name.c_str());
			return;
		}
	}
	//Non-group-by column in select list
	if(col_name != group_col){
		printf("Non-group-by column %s in select list\n",col_name.c_str());
		return;
	}
	int no_group = 0;
	if(group_col == "")
		no_group = 1;

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
			if(group_col_index == -1){
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
		if(group_col_index == -1){
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
		if(group_col_index == -1){
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
	if(agg_op.size() > 0)
    	cout << col_name << "|";
    else
    	cout << col_name << endl;
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
		MIN.push_back(INT_MIN); 
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
							if(agg_tbl[k]==1){
								string_copy(int2char_buffer,0,4, data_buffer1, tuple_offset1 + c_offset1[agg_col_index[k]], tuple_offset1 + c_offset1[agg_col_index[k]]+4);
							}
							else{
								string_copy(int2char_buffer,0,4, data_buffer2, tuple_offset2 + c_offset2[agg_col_index[k]], tuple_offset2 + c_offset2[agg_col_index[k]]+4);
							}	  		
			    			int next_equal_value = char_to_int(int2char_buffer);	
			    			// cout << "next_equal_value:   "   << next_equal_value <<endl;
							SUM[k] += next_equal_value;
							COUNT[k] += 1;
							double x = ((double)SUM[k]/(double)COUNT[k]);
							AVG[k] = (int)(x+0.5);
							MIN[k] = (next_equal_value < MIN[k])? next_equal_value : MIN[k]; 
							MAX[k] = (next_equal_value > MIN[k])? next_equal_value : MAX[k]; 
						}
					}
					else{
						insertkey2(join_col_value, join_col_value_int, page1, tuple_offset1, i, tuple_offset2, c_type2[join_col_index[1]]);
						
						delete_node.push_back(*next_equal_node);
						deletekey(join_col_value,join_col_value_int,c_type2[join_col_index[1]]);
						next_equal_node = lookupkey(join_col_value,join_col_value_int,c_type2[join_col_index[1]]);
					}
				}
				if(no_group == 0){
					for(k=0;k<delete_node.size();++k){
						insertkey(delete_node[k].key, delete_node[k].key_int, delete_node[k].page, delete_node[k].offset, delete_node[k].type);
					}
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
				cout << "|" <<endl;
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
				//cout << "SUM[j]: " << SUM[0] << "COUNT[j]: " << COUNT[0];
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
	clean2();
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

int file_size2(char* filename){  
    struct stat statbuf;  
    stat(filename,&statbuf);  
    int size=statbuf.st_size;  
  
    return size;  
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
