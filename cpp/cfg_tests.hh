#include <cxxtest/TestSuite.h>
#include "cfgfile.hh"
#include "binc.hh"

using namespace dds;
using std::string;
using binc::print;

using namespace Json;

namespace dds { class CfgTestSuite; }

class dds::CfgTestSuite : public  CxxTest::TestSuite
{
public:

	Value json_parse(const string& jstxt)
	{
		Value js;
		Reader reader;
		reader.parse(jstxt, js);
		TS_ASSERT(reader.good());

		return js;		
	}

	void test_json_stuff()
	{
		string json = 
		R"json(
		{
			"x": 1,
			"x": 2,
			"y": null,
			"z": {},
			"w": [],
			"s": "",
			"a": 1.E+0,                   // integral
			"b": 321321321321321321321321 // too large for integral
		}
		)json";

		Value js = json_parse(json);

		TS_ASSERT(! js["x"].isNull());
		TS_ASSERT(js["y"].isNull());
		TS_ASSERT(! js["z"].isNull());
		TS_ASSERT(! js["w"].isNull());

		TS_ASSERT(js["y"].empty());
		TS_ASSERT(js["z"].empty());
		TS_ASSERT(js["w"].empty());

		TS_ASSERT(js.isMember("x"));
		TS_ASSERT(js.isMember("y"));
		TS_ASSERT(! js.isMember("q"));

		TS_ASSERT_EQUALS(js["x"].asInt(), 2);
		TS_ASSERT(! js.get("y",true).asBool());
		TS_ASSERT(! js["s"].isNull());
		TS_ASSERT_EQUALS(js["a"].asString(),"1");

		TS_ASSERT_EQUALS( js["a"].type(), realValue);
		TS_ASSERT( js["a"].isIntegral() );
		TS_ASSERT( !js["b"].isIntegral() );
		TS_ASSERT_THROWS( js["b"].asLargestInt(), LogicError );
		TS_ASSERT_DELTA( js["b"].asDouble(), 321321321321321321321321.0, 1. );

	}


	typedef std::map<string,string> vmap;
	
	void check_purl(const string& url,
			const string& type, const string& path, const vmap& vars,
			open_mode mode, text_format format
			) {

 		parsed_url purl;

		parse_url(url, purl);
		
		TS_ASSERT_EQUALS(purl.type, type);
		TS_ASSERT_EQUALS(purl.path, path);
		TS_ASSERT_EQUALS(purl.vars, vars);
		TS_ASSERT_EQUALS(purl.mode, mode);
		TS_ASSERT_EQUALS(purl.format, format);

	}	
	
	void test_parse_url() {
		using namespace std;

		check_purl("file:a/v/hello.cc?x=1,y=hello world",
			   "file",
			   "a/v/hello.cc",
			   vmap { {"x", "1"}, {"y", "hello world"} }
			   ,default_open_mode
			   ,default_text_format
			);

		check_purl("file:/hello.cc?open_mode=truncate",
			   "file",
			   "/hello.cc",
			   vmap { {"open_mode", "truncate"}  }
			   ,open_mode::truncate
			   ,default_text_format
			);

		check_purl("hdf5:/hello.cc", "hdf5", "/hello.cc", vmap{} 
			   ,default_open_mode
			   ,default_text_format			
			);

		check_purl("hdf5:/hello.cc?open_mode=append,group=/foo/bar",
			   "hdf5",
			   "/hello.cc",
			   vmap { {"open_mode", "append"}, {"group", "/foo/bar"}  }
			   ,open_mode::append
			   ,default_text_format
			);

		check_purl("hdf5:hello?format=csvtab", "hdf5", "hello", vmap { {"format","csvtab"} }
			   ,default_open_mode
			   ,text_format::csvtab
			);
		check_purl("stdout:", "stdout", "", vmap { } 
			   ,default_open_mode
			   ,default_text_format
			);

		check_purl("foo:", "foo", "", vmap { } 
			   ,default_open_mode
			   ,default_text_format
			);
	}  

};
