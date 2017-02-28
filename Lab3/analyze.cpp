#include <fstream>
#include <iostream>
#include <vector>
using namespace std;

int pos;
vector <string> c_name;
vector <int> c_type;

string getword(string s, int l);
void create(string name, vector <string> cname, vector <int> ctype);
void analyze();

int main(int argc, char *argv[])
{
	analyze();
	return 0;
}

void analyze()
{
	string s = "insert into crtDemo values (1, 'first record');";
	string tmp;

	tmp = getword(s, pos);

	if(tmp == "create") //create
	{
		string tname;
		string next;
		string type;
		
		if(s.find("create", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("drop", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("insert", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("select", pos) != -1)
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
	}
	else if(tmp == "drop") //drop
	{
		string tname;

		if(s.find("create", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("drop", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("insert", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("select", pos) != -1)
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
	}
	else if(tmp == "insert") //insert
	{
		string tname;
		string next;

		if(s.find("create", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("drop", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("insert", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("select", pos) != -1)
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
	}
	else if(tmp == "select") //select
	{
		if(s.find("create", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("drop", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("insert", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		if(s.find("select", pos) != -1)
		{
			cout << "Syntax error" << endl;
			return;
		}
		next = getword(s, pos);
	}
	cout << "end" << endl;
	return;
}


string getword(string s, int l)
{
	int i, j;
	string tmp;
	for(i = l; i < s.length(); i++)
	{
		if(s[i] == ' ')
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
/*
		else if(s[i] == '\'')
		{
			pos++;
			return "\'";
		}
*/
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
