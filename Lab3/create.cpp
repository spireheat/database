#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
using namespace std;

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

        // int n = 10000;
        // unsigned char *p = (unsigned char*)&n;
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



