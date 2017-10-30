#ifndef __OUTPUT_TESTS_HH__
#define __OUTPUT_TESTS_HH__

#include <functional>
#include <typeinfo>
#include <regex>

#include "method.hh"
#include "binc.hh"
#include "cfgfile.hh"

#include <cxxtest/TestSuite.h>
#include "hdf5_util.hh"


using binc::print;
using namespace dds;


/*
  N.B. Some day, write up an injection facility...

 */

template <typename T, typename ...Args>
T* inject(Args...args)
{
	typedef factory<T, Args...> factory_type;
	
	auto factype = typeid(factory_type);
	return nullptr;
}





/*
template <>
inline agms_sketch_updater* 
inject<agms_sketch_updater, stream_id, agms::projection>(stream_id sid, 
	agms::projection proj)
{
	return agms_sketch_updater_factory(sid, proj);
}
*/


class MethodTestSuite : public CxxTest::TestSuite
{
public:

	void test_factory() {
		
	}

	typedef std::map<string,string> vmap;
	
	void check_purl(const string& url,
			const string& type, const string& path, const vmap& vars) {

 		parsed_url purl;

		parse_url(url, purl);
		
		TS_ASSERT_EQUALS(purl.type, type);
		TS_ASSERT_EQUALS(purl.path, path);
		TS_ASSERT_EQUALS(purl.vars, vars);
	}	
	
	void test_parse_url() {
		using namespace std;

		check_purl("file:a/v/hello.cc?x=1,y=hello world",
			   "file",
			   "a/v/hello.cc",
			   vmap { {"x", "1"}, {"y", "hello world"} }
			);

		check_purl("file:/hello.cc?open_mode=truncate",
			   "file",
			   "/hello.cc",
			   vmap { {"open_mode", "truncate"}  }
			);

		check_purl("hdf5:/hello.cc", "hdf5", "/hello.cc", vmap{} );

		check_purl("hdf5:/hello.cc?open_mode=truncate,group=/foo/bar",
			   "hdf5",
			   "/hello.cc",
			   vmap { {"open_mode", "truncate"}, {"group", "/foo/bar"}  }
			);

		check_purl("hdf5:hello", "hdf5", "hello", vmap { });
		check_purl("stdout:", "stdout", "", vmap { } );

		check_purl("foo:", "foo", "", vmap { } );


	}  
};


#endif
