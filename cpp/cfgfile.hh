#ifndef _CFGFILE_HH_
#define _CFGFILE_HH_

#include <vector>
#include <map>

#include <jsoncpp/json/json.h>

#include "agms.hh"
#include "method.hh"
#include "query.hh"

/**
   Utilities to read json files using libjsoncpp
*/

namespace dds
{
	using std::vector;

	/**
		\brief Return a projection from the current object.

		This will look for a member of the form
		\code{.cpp}
		"projection": {
			"depth": <int>,
			"width": <int>,
			["epsilon": <float>]
		}
		\endcode
	  */
	agms::projection get_projection(const Json::Value& js);

	/**
		\brief Return a set of sids from the current object.

		This will look for a member of the form
		\code{.cpp}
		"stream:": <int>
		\endcode
		or
		\code{.cpp}
		"streams": <int>
		\endcode
		or
		\code{.cpp}
		"streams": [<int> *]   // zero streams is acceptable!
		\endcode
	  */
	std::vector<stream_id> get_streams(const Json::Value& js);
 
	/**
		\brief Return a set of sids from the current object.

		This will look for a member of the form
		\code{.cpp}
		"streams": <int>
		\endcode
		or
		\code{.cpp}
		"streams": [<int> *]   // zero streams is acceptable!
		\endcode
	  */
	basic_stream_query get_query(const Json::Value& js);


	typedef std::unordered_map<std::string, output_file*> output_file_map;

	struct parsed_url
	{
		string type;
		string path;
		std::map<string, string> vars;
		open_mode   mode = default_open_mode;
		text_format format = tables::default_text_format;
	};

	void parse_url(const string& url, parsed_url& purl);
	
	void prepare_dataset(Json::Value& cfg, dataset& D);
	void prepare_components(Json::Value&, vector<component*>& comp);
	output_file_map prepare_output(Json::Value&, reporter&);

	void execute(Json::Value&);


	void generate_schema(output_table* table);

 
} // end namespace dds

#endif
