#include <cstdio>
#include <cstdlib>
#include <fstream>

#include "cfgfile.hh"

// Include all components (!#$!@#%% linker!!)
#include "accurate.hh"
#include "tods.hh"
#include "gm.hh"
//#include "sgm.hh"
//#include "fgm.hh"

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
	c.push_back(&gm::frgm::frgm_comptype);
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

void generate_schemas()
{
	cout << "Generating schemas:" << endl;
	for(auto&& t : output_table::all()) {
		cout << "   " << t->name() << endl;
		generate_schema(t);
	}
}


int main(int argc, char** argv)
{
	Json::Value cfg;

	if(argc!=2) 
	{
		cerr << "Expected config file argument:  <mycfg>.json" << endl;
		usage();
		return 1;
	}

	if(argc==2) 
	{
		if(string(argv[1])=="--output-schemas") {
			generate_schemas();

		} else {

			ifstream cfgfile(argv[1]);
			if(!cfgfile.good()) {
				cerr << "Cannot open json file: " << argv[1] << endl;
				return 1;
			}

			cfgfile >> cfg;		
			execute(cfg);
		}

		return 0;
	} 

	return 1;

}
