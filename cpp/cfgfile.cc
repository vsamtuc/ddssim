
#include <vector>
#include <regex>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <fstream>
#include <typeinfo>

#include "data_source.hh"
#include "accurate.hh"
#include "tods.hh"
#include "agms.hh"
#include "agm.hh"
#include "gm.hh"
#include "cfgfile.hh"
#include "binc.hh"
#include "output.hh"
#include "results.hh"


using namespace dds;

using std::vector;

using Json::Value;
using agms::projection;
using agms::depth_type;
using agms::index_type;

using binc::print;
using binc::elements_of;


static datasrc proc_datasrc(Value& jdset)
{
	string HOME(getenv("HOME"));
	string fname = HOME+jdset["file"].asString();

	string driver = jdset.get("driver", "wcup").asString();
	datasrc ds;
	if(driver=="wcup")
		ds = wcup_ds(fname);
	else if(driver=="hdf5") {
		string dsetname = jdset.get("dataset","ddstream").asString();
		ds = hdf5_ds(fname, dsetname);
	} else
		throw std::runtime_error("Error: unknown data source driver:" + driver);
	return ds;
}


void dds::prepare_dataset(Value& cfg, dataset& D)
{
	Json::Value jdset = cfg["dataset"];
	
	if(jdset.isNull())
		return;

	set<string> kwords {
		"driver", "file", "dataset", 
		"set_max_length", "hash_sources", "hash_streams", "time_window",
		"warmup_time", "warmup_size", "cooldown"
	};
	for(auto member: jdset.getMemberNames()) {
		if(kwords.count(member)==0)
			throw std::runtime_error("Unknown keyword `"+member+"' in dataset section of config");
	}	

	datasrc ds = proc_datasrc(jdset);
	
	D.load(ds);

	{
		Json::Value js = jdset["set_max_length"];
		if(!js.isNull())
			D.set_max_length(js.asUInt());
	}
	{
		Json::Value js = jdset["hash_sources"];
		if(!js.isNull())
			D.hash_sources(js.asInt());
	}
	{
		Json::Value js = jdset["hash_streams"];
		if(!js.isNull())
			D.hash_streams(js.asInt());
	}
	{
		Json::Value js = jdset["time_window"];
		if(!js.isNull())
			D.set_time_window(js.asInt());
	}
	{
		Json::Value js = jdset["warmup_time"];
		if(!js.isNull()) {
			bool cool = jdset.get("cooldown",true).asBool();
			D.warmup_time(js.asInt(), cool);
		}
	}
	{
		Json::Value js = jdset["warmup_size"];
		if(!js.isNull()) {
			bool cool = jdset.get("cooldown",true).asBool();
			D.warmup_size(js.asInt(), cool);
		}
	}

	D.create();
}



static projection get_projection(Value& js)
{
	Value& jp = js["projection"];

	depth_type d = jp["depth"].asInt();
	index_type w = jp["width"].asInt();

	assert(d!=0 && w!=0);
	
	projection proj(d,w);

	if(!jp["epsilon"].isNull())
		proj.set_epsilon(jp["epsilon"].asDouble());
	return proj;
}

static double get_beta(Value& js)
{
	return js["beta"].asDouble();
}

static stream_id get_stream(Value& js)
{
	return js["stream"].asInt();
}


static void handle_agm(Value& js, vector<reactive*> components)
{
	stream_id sid = get_stream(js);
	projection proj = get_projection(js);
	double beta = get_beta(js);
	components.push_back(new agm::network(sid, proj, beta ));
}

static void handle_gm(Value& js, vector<reactive*> components)
{
	stream_id sid = get_stream(js);
	projection proj = get_projection(js);
	double beta = get_beta(js);
	components.push_back(new gm::network(sid, proj, beta ));
}




void dds::prepare_components(Value& js, vector<reactive*>& components)
{
	Value jcomp = js["components"];

	if(jcomp.isNull()) return;
	if(! jcomp.isArray())
		throw std::runtime_error("Error in cfg file: 'components' is not an array");

	for(size_t index = 0; index < jcomp.size(); index ++) {
		Value jc = jcomp[(int) index];

		string type = jc["type"].asString();

		// map to a handler
		if(type=="agm")
			handle_agm(jc, components);
		else if (type=="gm")
			handle_gm(jc, components);
		else
			throw std::runtime_error("Error: component type '"+type+"' is unknown");
	}

}


static open_mode proc_open_mode(const map<string, string>& vars)
{
	open_mode omode = default_open_mode;
	if(vars.count("open_mode")) {
		string om = vars.at("open_mode");
		if(om=="truncate")
			omode = open_mode::truncate;
		else if (om=="append")
			omode = open_mode::append;
		else
			throw std::runtime_error("Illegal value in URL: open_mode="+om);
	}
	return omode;
}



#define RE_FNAME "[a-zA-X0-9 _.-]+"
#define RE_PATH "(/?(?:" RE_FNAME "/)*(?:" RE_FNAME "))"
#define RE_ID   "[a-zA-Z_][a-zA-Z0-9_]*"
#define RE_TYPE "(" RE_ID  "):"
#define RE_VAR  RE_ID "=" RE_PATH
#define RE_VARS RE_VAR "(?:," RE_VAR ")*" 
#define RE_URL  RE_TYPE RE_PATH "?(?:\\?(" RE_VARS "))?" 

void dds::parse_url(const string& url, parsed_url& purl)
{
	using std::regex;
	using std::smatch;
	using std::regex_match;
	
	regex re_url(RE_URL);
	smatch match;
	
	if(! regex_match(url, match, re_url))
		throw std::runtime_error("Malformed url `"+url+"'");

	purl.type = match[1];
	purl.path = match[2];
	string allvars = match[3];

	// split variables

	regex re_var( "(" RE_ID ")=(" RE_PATH ")" );
	regex re_comma(",");
	auto s = sregex_token_iterator(allvars.begin(), allvars.end(), re_comma, -1);
	auto s2 = sregex_token_iterator();
	for(; s!=s2; ++s) {
		string var =*s;
		smatch vmatch;
		regex_match(var, vmatch, re_var);
		string vname = vmatch[1];
		string vvalue = vmatch[2];
		if(! vname.empty()) purl.vars[vname] = vvalue; 
	}
}


static output_file* process_output_file(const string& url)
{
	parsed_url purl;

	parse_url(url, purl);
	
	if(purl.type == "file")
		return CTX.open(purl.path, proc_open_mode(purl.vars));
	else if (purl.type == "hdf5")
		return CTX.open_hdf5(purl.path, proc_open_mode(purl.vars));
	else if (purl.type == "stdout")
		return &output_stdout;
	else if (purl.type == "stderr")
		return &output_stderr;
	throw std::runtime_error("Unknown output_file type: `"+purl.type+"'");
}


static void proc_bind(output_table* tab, const string& fname, const output_file_map& fmap)
{
	if(fmap.count(fname)==0)
		throw std::runtime_error("Could not find file `"+fname+"' to bind table `"+tab->name()+"' to.");
	tab->bind(fmap.at(fname));
}

output_file_map dds::prepare_output(Json::Value& jsctx, reporter& R)
{
	output_file_map fmap;

	// check the relevant sections, "files" and "bind"
	Value files = jsctx["files"];

	for(auto member : files.getMemberNames()) {
		string url = files[member].asString();
		fmap[member] = process_output_file(url);
	}

	// do the bindings
	Value bind = jsctx["bind"];
	for(auto table_name : bind.getMemberNames()) {
		output_table* table = output_table::get(table_name);
		Value binds = bind[table_name];
		if(binds.isNull())
			continue;
		else if(binds.isString())
			proc_bind(table, binds.asString(), fmap);
		else if(binds.isArray()) {
			for(size_t i=0; i<binds.size(); i++)
				proc_bind(table, binds[(int)i].asString(), fmap);
		} else
			throw std::runtime_error("Binding for `"+table->name()+"' is not a string or array");

		if(table->flavor()==table_flavor::RESULTS)
			R.watch(*table);
	}

	// set up sampling
	Value sample = jsctx["sample"];
	for(auto ts_name : sample.getMemberNames()) {
		time_series* ts = dynamic_cast<time_series*>(output_table::get(ts_name));
		if(ts == nullptr || ts->flavor()!=table_flavor::TIMESERIES)
			throw std::runtime_error("Could not find time series table `"+ts_name+"'");
		R.sample(*ts, sample[ts_name].asUInt());
	}
	
	return fmap;
}


void dds::execute(Value& cfg)
{
	/* Create dataset */
	dataset D;
	prepare_dataset(cfg, D);
	
	/* Create components */
	std::vector<reactive*> components;
	prepare_components(cfg, components);
	
	/* Create output files */
	reporter R;
	prepare_output(cfg, R);

	progress_reporter pbar(stdout, 40, "Progress: ");

	/* Run */
	using namespace std::chrono;
	steady_clock::time_point startt = steady_clock::now();
	CTX.run();
	steady_clock::time_point endt = steady_clock::now();
	cout << "Execution time=" 
		<< duration_cast<milliseconds>(endt-startt).count()/1000.0
		<< "sec" << endl;

	/* Clean up */
	for(auto p : components)
		delete p;
	CTX.close_result_files();
	agms_sketch_updater_factory.clear();	
}


