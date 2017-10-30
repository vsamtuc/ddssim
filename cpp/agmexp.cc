
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <fstream>
#include <typeinfo>


#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"
#include "output.hh"
#include "results.hh"
#include "tods.hh"
#include "agm.hh"
#include "gm.hh"
#include "cfgfile.hh"

using namespace dds;


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
	return 0;
}



