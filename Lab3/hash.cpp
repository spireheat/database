#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

using namespace std;

typedef struct NODE{
	char* key;
	int key_int;
	int page;
	int offset;
	int type;
	struct NODE *next;
}NODE;

#define KEYLEN 100
#define HASHSIZE 8192
static struct NODE *HASHTABLE[HASHSIZE];
struct NODE* mallocnode(string key, int key_int, int page, int offset,int type);
unsigned int hash(char *str);
int insertkey(char *key, int key_int, int page, int offset, int type);
struct NODE * lookupkey(char *key, int key_int, int type);
int deletekey(char *key, int key_int, int type);
void ITOA(int num,char* str,int radix);
void clean();
long long mspace=0;

// int main() {
// 	clean();
// 	return 0;
// }

struct NODE *mallocnode(char* key, int key_int, int page, int offset, int type) {
	NODE *node;

	node = (NODE*) malloc(sizeof(NODE));
	mspace+=sizeof(NODE);

	if(node == NULL)
		return NULL;

	node->key = strdup(key);
	mspace+=(strlen(key)+1);
	node->key_int = key_int;
	mspace+=(5);
	node->page = page;
	mspace+=(5);
	node->offset = offset;
	mspace+=(5); 
	node->type = type;
	mspace+=(5); 
	node->next = NULL;
	return node;
}

unsigned int hash(char *str)
{
    unsigned int seed = 131;
    unsigned int hash = 0;
    while (*str)
    {
        hash = hash * seed + (*str++);
    }
 	//cout << "hash:  " << (hash & 0x7FFFFFFF)%HASHSIZE <<endl;
    return (hash & 0x7FFFFFFF)%HASHSIZE;
}


int insertkey(char *key, int key_int, int page, int offset, int type) {
	int hashvalue;
	char str[100] = {0};
	if(type == 0){
		ITOA(key_int,str,10);
		hashvalue = hash(str);
	}
	else{
		hashvalue = hash(key);
	}

	struct NODE *node;

	node = mallocnode(key, key_int, page, offset, type);

	if(node == NULL) {
		return 0;
	}
	else {
		node->next = HASHTABLE[hashvalue];
		HASHTABLE[hashvalue] = node;
		return 1;
	}
	return -1;
}


struct NODE* lookupkey(char *key, int key_int, int type) {
	unsigned int hashvalue;
	struct NODE *node;

	char str[100] = {0};
	if(type == 0){
		ITOA(key_int,str,10);
		hashvalue = hash(str);
	}
	else{
		hashvalue = hash(key);
	}

	node = HASHTABLE[hashvalue];

	for(; node != NULL; node = node->next) {
		if(type == 0){	//int
			if(key_int == node->key_int){
				//cout << "successfully find the node:  " << key_int << endl;
				return node;
			}
		}
		else{
			if(strcmp(key, node->key) == 0) {
				//cout << "successfully find the node" << key <<endl;
				return node;
			}
		}
	}
	//cout << "FIND NULL RETURN" << endl;
	return NULL;
}


int	deletekey(char *key, int key_int, int type) {
	unsigned int hashvalue;
	struct NODE *node;
	struct NODE *pre;

	char str[100] = {0};
	if(type == 0){
		ITOA(key_int,str,10);
		hashvalue = hash(str);
	}
	else{
		hashvalue = hash(key);
	}

	node = HASHTABLE[hashvalue];
	pre = HASHTABLE[hashvalue];

	for(; node != NULL; node = node->next) {
		if((type == 0 && key_int == node->key_int) || (type == 1 && strcmp(key, node->key) == 0)){
			if(HASHTABLE[hashvalue]->next == NULL) {
				free(HASHTABLE[hashvalue]->key);
				free(HASHTABLE[hashvalue]);
				HASHTABLE[hashvalue] = NULL;
			}

			else if(HASHTABLE[hashvalue] == node) {
				node = HASHTABLE[hashvalue]->next;
				free(HASHTABLE[hashvalue]->key);
				free(HASHTABLE[hashvalue]);
				HASHTABLE[hashvalue] = node;
			}

			else {
				while(pre != NULL && pre->next != node)
					pre = pre->next;
				pre->next = node->next;
				free(node->key);
				free(node);
			}
			return 1;
		}		
	}
	return 0;
}

void clean() {
	int i;
	struct NODE *node;
	struct NODE *next;

	for(i = 0; i < HASHSIZE; i++) {
		if(HASHTABLE[i] != NULL) {
			node = HASHTABLE[i];
			while(node != NULL) {
				next = node->next;
				free(node->key);
				free(node);
				node = next;
			}
		}
		HASHTABLE[i] = NULL;
	}
}


void ITOA(int num,char* str,int radix){
	char index[]="0123456789ABCDEF";
	unsigned unum;
	int i=0,j,k;

	if(radix==10&&num<0){
		unum=(unsigned)-num;
		str[i++]='-';
	}
	else 
	unum=(unsigned)num;
	do{
		str[i++]=index[unum%(unsigned)radix];
		unum/=radix;
	}while(unum);

	str[i]='\0';

	if(str[0]=='-')
		k=1;
	else 
		k=0;

	char temp;
	for(j=k;j<=(i-1)/2;j++){
		temp=str[j];
		str[j]=str[i-1+k-j];
		str[i-1+k-j]=temp;
	}
}