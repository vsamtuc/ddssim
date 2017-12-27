
#include <vector>
#include <regex>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <fstream>
#include <typeinfo>

#include <boost/core/demangle.hpp>

#include "data_source.hh"
#include "accurate.hh"
#include "cfgfile.hh"
#include "method.hh"
#include "output.hh"
#include "results.hh"
#include "binc.hh"


using namespace dds;

using std::vector;
using std::initializer_list;

using Json::Value;
using agms::projection;
using agms::depth_type;

using binc::print;
using binc::elements_of;



//----------------------------------------------
//
//  Processing the "dataset" section
//
//----------------------------------------------



void dds::prepare_dataset(Value& cfg, dataset& D)
{
	Json::Value jdset = cfg["dataset"];
	
	if(jdset.isNull())
		return;

	set<string> kwords {
		"data_source", // this is a url
		"loops", "max_length", "max_timestamp", "hash_sources", "hash_streams", 
		"time_window", "fixed_window", "flush_window",
		"warmup_time", "warmup_size"
	};
	for(auto member: jdset.getMemberNames()) {
		if(kwords.count(member)==0)
			throw std::runtime_error("Unknown keyword `"+member+"' in dataset section of config");
	}	

	if(! jdset.isMember("data_source"))
		throw std::runtime_error("The dataset does not specify some data_source");

	parsed_url purl;
	parse_url(jdset["data_source"].asString(), purl);

	datasrc ds = open_data_source(purl.type, purl.path, purl.vars);
	
	D.load(ds);

	{
		Json::Value js = jdset["loops"];
		if(!js.isNull())
			D.set_loops(js.asUInt());
	}
	{
		Json::Value js = jdset["max_length"];
		if(!js.isNull())
			D.set_max_length(js.asUInt());
	}
	{
		Json::Value js = jdset["max_timestamp"];
		if(!js.isNull())
			D.set_max_timestamp(js.asInt());
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
		bool flush = jdset.get("flush_window",false).asBool();
		if(!js.isNull())
			D.set_time_window(js.asInt(), flush);
	}
	{
		Json::Value js = jdset["fixed_window"];
		bool flush = jdset.get("flush_window",false).asBool();
		if(!js.isNull())
			D.set_fixed_window(js.asInt(), flush);
	}
	{
		Json::Value js = jdset["warmup_time"];
		if(!js.isNull()) {
			D.warmup_time(js.asInt());
		}
	}
	{
		Json::Value js = jdset["warmup_size"];
		if(!js.isNull()) {
			D.warmup_size(js.asInt());
		}
	}

	D.create();
}


//----------------------------------------------
//
//  Processing the "components" section
//
//----------------------------------------------


projection dds::get_projection(const Value& js)
{
	const Value& jp = js["projection"];

	depth_type d = jp["depth"].asUInt();
	size_t w = jp["width"].asUInt();
	assert(d>0 && w>0);
	
	projection proj(d,w);

	if(!jp["epsilon"].isNull())
		proj.set_epsilon(jp["epsilon"].asDouble());
	return proj;
}

std::vector<stream_id> dds::get_streams(const Value& js)
{
	std::vector<stream_id> ret;

	if(js.isMember("stream")) {
		ret.push_back( js["stream"].asInt() );
	} else if(js.isMember("streams")) {
		const Value& jp = js["streams"];
		if(jp.isArray()) {
			for(auto&& val : jp) {
				ret.push_back(val.asInt());
			}
		}
		else {
			ret.push_back(jp.asInt());
		}		
	}

	return ret;
}


basic_stream_query dds::get_query(const Value& js)
{
	basic_stream_query Q;
	if(! js.isMember("query"))
		return Q;
	qtype qt = qtype_repr[js["query"].asString()];
	Q.set_type(qt);

	double beta = js.get("beta", 0.0).asDouble();
	Q.set_approximation(beta);

	auto streams = get_streams(js);
	Q.set_operands(streams);

	return Q;
}


void dds::prepare_components(Value& js, vector<component*>& components)
{
	Value jcomp = js["components"];

	if(jcomp.isNull()) return;
	if(! jcomp.isArray())
		throw std::runtime_error("Error in cfg file: 'components' is not an array");

	for(size_t index = 0; index < jcomp.size(); index ++) {
		Value jc = jcomp[(int) index];

		string type = jc["type"].asString();

		// map to a handler
		basic_component_type* ctype = basic_component_type::get_component_type(type);
		try {
			auto c = ctype->create(jc);
			cout << "Component " << c->name() << " of component type " << type << "(instance of " 
				<< boost::core::demangle(typeid(*c).name()) << ") created"<<endl;
			components.push_back(c);			
		} catch(std::exception& e) {
			std::cerr << "Failed to create component " << index << std::endl;
			throw;
		}
	}
}

//------------------------------------
//
//  Processing the "files" section
//
//------------------------------------

template <typename T>
struct enum_processor 
{
	string varname;
	T default_value;
	enum_repr<T>& repr;

	enum_processor(const string& _vname, T _defval, enum_repr<T>& _repr)
		: varname(_vname), default_value(_defval), repr(_repr)
	{ }

	T operator()(const map<string, string>& vars) 
	{
		T retval = default_value;
		if(vars.count(varname)) {
			string om = vars.at(varname);
			if(! repr.is_member(om))
				throw std::runtime_error("Illegal value in URL: "+varname+"="+om);
			retval = repr[om];
		}
		return retval;
	}
};


using namespace std::string_literals;

enum_processor<text_format> proc_text_format("format"s, default_text_format, text_format_repr);
enum_processor<open_mode> proc_open_mode("open_mode"s, default_open_mode, open_mode_repr);


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
	using std::sregex_token_iterator;
	
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

	purl.mode = proc_open_mode(purl.vars);
	purl.format = proc_text_format(purl.vars);
}


static output_file* process_output_file(const string& url)
{
	parsed_url purl;

	parse_url(url, purl);
	
	if(purl.type == "file")
		return CTX.open(purl.path, purl.mode, purl.format);
	else if (purl.type == "hdf5")
		return CTX.open_hdf5(purl.path, purl.mode);
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


//------------------------------------
//
//  Execution
//
//------------------------------------


void dds::execute(Value& cfg)
{
	/* Reset the context */
	CTX.initialize();

	/* Create dataset */
	dataset D;
	prepare_dataset(cfg, D);
	
	/* Create components */
	std::vector<component*> components;
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
	CTX.clear();
}



void dds::generate_schema(output_table* table)
{
	using std::ofstream;
	using std::ios_base;

	/// create the output file name (json)
	string filename = table->name()+".schema";

	Json::Value root, columns;

	root["name"] = table->name();
	for(size_t i=0; i<table->size(); i++) {
		Value column;
		column["name"] = (*table)[i]->name();
		column["type"] = boost::core::demangle((*table)[i]->type().name());
		column["arithmetic"] = (*table)[i]->is_arithmetic();

		root["columns"][Json::ArrayIndex(i)] = column;
	}

	ofstream scfile(filename, ios_base::out|ios_base::trunc);
	scfile << root << endl;

	scfile.close();
	// close by returning
}