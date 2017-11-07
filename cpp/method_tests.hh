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

};


#endif
