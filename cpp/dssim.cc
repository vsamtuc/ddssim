#include <cstdio>
#include <cstdlib>
#include <fstream>

#include "cfgfile.hh"

// Include all components (!#$!@#%% linker!!)
#include "accurate.hh"
#include "tods.hh"
#include "sgm.hh"
#include "fgm.hh"

using namespace dds;
using namespace std;




void usage()
{
	// Here, we do not use the component registry, in order to force
	// the !#$!@#%^ linker to link all components!
	vector<basic_component_type*> c;
	c.push_back(&tods::tods_comptype);
	c.push_back(&gm::sgm::sgm_comptype);
	c.push_back(&gm::fgm::fgm_comptype);
	c.push_back(&dds::data_source_statistics::comp_type);
	c.push_back(&dds::exact_query_comptype);
	c.push_back(&dds::agms_query_comptype);

	cout << "Components:" << endl;
	for(auto&& c: basic_component_type::component_types())
		cout << "   " << c.first << endl;

	cout << "Output tables" <<endl;
	for(auto&& t : output_table::all())
		cout << "   " << t->name() << endl;

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
