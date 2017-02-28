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


