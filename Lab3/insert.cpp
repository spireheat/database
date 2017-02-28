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

int string_copy(char* , int, int, char* , int, int);
int char_to_int(char p[4]);
char* int_to_char(int n);
int file_size2(char* filename);
char a[4];

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



int main(){
    string name = "mytest";
    vector <string> c_name;
    vector <int> c_type;
    c_name.push_back("ID");
    c_name.push_back("name");
    c_name.push_back("addr");
    c_name.push_back("phone");
    c_type.push_back(0);
    c_type.push_back(1);
    c_type.push_back(1);
    c_type.push_back(0);
    vector <string> value;
    value.push_back("0001");
    value.push_back("'xiaoming'");
    value.push_back("'beijing'");
    value.push_back("10086");
    insert(name,value);
    value.clear();
    value.push_back("0002");
    value.push_back("'xiaohong'");
    value.push_back("'tianjing'");
    value.push_back("10000");
    insert(name,value);   

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
