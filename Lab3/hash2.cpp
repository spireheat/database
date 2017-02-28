#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

using namespace std;

typedef struct NODE2{
	char* key;
	int key_int;
	int page1;
	int offset1;
	int page2;
	int offset2;
	int type;
	struct NODE2 *next;
}NODE2;

#define KEYLEN 100
#define HASHSIZE 8192
static struct NODE2 *HASHTABLE2[HASHSIZE];
struct NODE2* mallocnode2(string key, int key_int, int page1, int offset1, int page2, int offset2, int type);
unsigned int hash(char *str);
int insertkey2(char *key, int key_int, int page1, int offset1, int page2, int offset2, int type);
struct NODE2 * lookupkey2(char *key, int key_int, int type);
int deletekey2(char *key, int key_int, int type);
void ITOA(int num,char* str,int radix);
void clean2();
//long long mspace=0;

// int main() {
// 	clean();
// 	return 0;
// }

struct NODE2 *mallocnode2(char* key, int key_int, int page1, int offset1, int page2, int offset2, int type) {
	struct NODE2 *node;

	node = (NODE2 *) malloc(sizeof(NODE2));
	mspace+=sizeof(NODE2);

	if(node == NULL)
		return NULL;

	node->key = strdup(key);
	mspace+=(strlen(key)+1);
	node->key_int = key_int;
	mspace+=(5);
	node->page1 = page1;
	mspace+=(5);
	node->offset1 = offset1;
	mspace+=(5); 
	node->page2 = page2;
	mspace+=(5);
	node->offset2 = offset2;
	mspace+=(5); 
	node->type = type;
	mspace+=(5); 
	node->next = NULL;
	return node;
}

// unsigned int hash(char *str)
// {
//     unsigned int seed = 131;
//     unsigned int hash = 0;
//     while (*str)
//     {
//         hash = hash * seed + (*str++);
//     }
//  	//cout << "hash:  " << (hash & 0x7FFFFFFF)%HASHSIZE <<endl;
//     return (hash & 0x7FFFFFFF)%HASHSIZE;
// }


int insertkey2(char *key, int key_int, int page1, int offset1, int page2, int offset2, int type) {
	int hashvalue;
	char str[100] = {0};
	if(type == 0){
		ITOA(key_int,str,10);
		hashvalue = hash(str);
	}
	else{
		hashvalue = hash(key);
	}

	struct NODE2 *node;

	node = mallocnode2(key, key_int, page1, offset1, page2, offset2, type);

	if(node == NULL) {
		return 0;
	}
	else {
		node->next = HASHTABLE2[hashvalue];
		HASHTABLE2[hashvalue] = node;
		return 1;
	}
	return -1;
}


struct NODE2* lookupkey2(char *key, int key_int, int type) {
	unsigned int hashvalue;
	struct NODE2 *node;

	char str[100] = {0};
	if(type == 0){
		ITOA(key_int,str,10);
		hashvalue = hash(str);
	}
	else{
		hashvalue = hash(key);
	}

	node = HASHTABLE2[hashvalue];

	for(; node != NULL; node = node->next) {
		if(type == 0){	//int
			if(key_int == node->key_int){
				//cout << "successfully find the NODE2:  " << key_int << endl;
				return node;
			}
		}
		else{
			if(strcmp(key, node->key) == 0) {
				//cout << "successfully find the NODE2" << key <<endl;
				return node;
			}
		}
	}
	//cout << "FIND NULL RETURN" << endl;
	return NULL;
}


int	deletekey2(char *key, int key_int, int type) {
	unsigned int hashvalue;
	struct NODE2 *node;
	struct NODE2 *pre;

	char str[100] = {0};
	if(type == 0){
		ITOA(key_int,str,10);
		hashvalue = hash(str);
	}
	else{
		hashvalue = hash(key);
	}

	node = HASHTABLE2[hashvalue];
	pre = HASHTABLE2[hashvalue];

	for(; node != NULL; node = node->next) {
		if((type == 0 && key_int == node->key_int) || (type == 1 && strcmp(key, node->key) == 0)){
			if(HASHTABLE2[hashvalue]->next == NULL) {
				free(HASHTABLE2[hashvalue]->key);
				free(HASHTABLE2[hashvalue]);
				HASHTABLE2[hashvalue] = NULL;
			}

			else if(HASHTABLE2[hashvalue] == node) {
				node = HASHTABLE2[hashvalue]->next;
				free(HASHTABLE2[hashvalue]->key);
				free(HASHTABLE2[hashvalue]);
				HASHTABLE2[hashvalue] = node;
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

void clean2() {
	int i;
	struct NODE2 *node;
	struct NODE2 *next;

	for(i = 0; i < HASHSIZE; i++) {
		if(HASHTABLE2[i] != NULL) {
			node = HASHTABLE2[i];
			while(node != NULL) {
				next = node->next;
				free(node->key);
				free(node);
				node = next;
			}
		}
		HASHTABLE2[i] = NULL;
	}
}


// void ITOA(int num,char* str,int radix){
// 	char index[]="0123456789ABCDEF";
// 	unsigned unum;
// 	int i=0,j,k;

// 	if(radix==10&&num<0){
// 		unum=(unsigned)-num;
// 		str[i++]='-';
// 	}
// 	else 
// 	unum=(unsigned)num;
// 	do{
// 		str[i++]=index[unum%(unsigned)radix];
// 		unum/=radix;
// 	}while(unum);

// 	str[i]='\0';

// 	if(str[0]=='-')
// 		k=1;
// 	else 
// 		k=0;

// 	char temp;
// 	for(j=k;j<=(i-1)/2;j++){
// 		temp=str[j];
// 		str[j]=str[i-1+k-j];
// 		str[i-1+k-j]=temp;
// 	}
// }