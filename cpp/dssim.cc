#include <cstdio>
#include <cstdlib>
#include <fstream>

#include "cfgfile.hh"

using namespace dds;
using namespace std;

void usage()
{
	cout << "SUpported components:" << endl;
	for(auto&& c: basic_component_type::component_types())
		cout << "   " << c.first << endl;
}

int main(int argc, char** argv)
{
	Json::Value cfg;
	if(argc>1) {
		fstream cfgfile(argv[1]);
		cfgfile >> cfg;		
	} else {
		cout << "Expected config file argument:  <mycfg>.json" << endl;
		usage();
		return -1;
	}

	execute(cfg);
	return 0;
}
