#ifndef _CFGFILE_HH_
#define _CFGFILE_HH_

#include <vector>
#include <map>

#include <jsoncpp/json/json.h>

#include "method.hh"

/**
   Utilities to read json files using libjsoncpp
*/

namespace dds
{
	using std::vector;

	typedef std::unordered_map<std::string, output_file*> output_file_map;


	struct parsed_url
	{
		string type;
		string path;
		std::map<string, string> vars;
		open_mode   mode = default_open_mode;
		text_format format = default_text_format;
	};

	void parse_url(const string& url, parsed_url& purl);
	
	void prepare_dataset(Json::Value& cfg, dataset& D);
	void prepare_components(Json::Value&, vector<reactive*>& comp);
	output_file_map prepare_output(Json::Value&, reporter&);

	void execute(Json::Value&);
  
} // end namespace dds

#endif
