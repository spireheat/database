#include <fstream>
#include <iostream>
#include <vector>
using namespace std;
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