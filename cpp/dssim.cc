#include <cstdio>
#include <cstdlib>
#include <fstream>

#include "cfgfile.hh"

using namespace dds;
using namespace std;

int main(int argc, char** argv)
{
	Json::Value cfg;
	if(argc>1) {
		fstream cfgfile(argv[1]);
		cfgfile >> cfg;		
	} else {
		cout << "Expected config file argument:  <mycfg>.json" << endl;
		return -1;
	}

	execute(cfg);
	execute(cfg);
	return 0;
}
