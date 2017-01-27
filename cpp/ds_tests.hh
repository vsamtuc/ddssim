#ifndef _DS_TESTS_HH_
#define _DS_TESTS_HH_

#include <cxxtest/TestSuite.h>
#include <unordered_set>
#include <algorithm>
#include <random>
#include <sstream>
#include "data_source.hh"

using std::unordered_set;
using std::min_element;
using std::max_element;
using std::mt19937;

class DataSourceTestSuite : public CxxTest::TestSuite
{
public:

	void test_generated()
	{
		data_source* ds = generated_ds([](dds::dds_record rec)->bool{
			rec.sid=0;
			rec.hid=0;
			rec.sop=dds::INSERT;
			rec.ts=10;
			rec.key=20;
			return true;
		});

		for(int i=0;i<1000;i++) {
			TS_ASSERT(ds->valid());
			TS_ASSERT(ds->get().sop == dds::INSERT);
			ds->advance();
		}
		delete ds;
	}


	size_t ds_length(data_source* ds) {
		size_t count = 0;
		while(ds->valid()) {
			count++;
			ds->advance();
		}
		return count;		
	}

	void test_func_max_length()
	{
		{
			dsref ds { generated_ds(max_length(10)) };
			TS_ASSERT_EQUALS(ds_length(ds.get()) , 10);
		}
		{
			dsref ds { generated_ds(max_length(20)) };
			TS_ASSERT_EQUALS(ds_length(ds.get()) , 20);			
		}
		{
			auto F = max_length(10) | max_length(20);
			dsref ds { generated_ds(F) };			
			TS_ASSERT_EQUALS(ds_length(ds.get()) , 10);			
		}
	}

	void test_func_set_attr()
	{
		mt19937 rng(12344);
		auto F = max_length(10) 
			| set_attr(& dds::dds_record::key, rng, 
				std::uniform_int_distribution<dds::key_type>(50,100))
			| set_attr(& dds::dds_record::sid, 0)
			| inc_attr(& dds::dds_record::hid, 0, 1)
			| set_attr(& dds::dds_record::sop, dds::INSERT)
			| inc_attr(& dds::dds_record::ts, (dds::timestamp)40, rng,
				std::uniform_int_distribution<dds::timestamp>(2,10))
			;

		dsref ds { 
			time_window(generated_ds(F), 15)
		};
		for(; ds->valid(); ds->advance()) {
			TS_ASSERT_LESS_THAN_EQUALS(50, ds->get().key);
			TS_ASSERT_LESS_THAN_EQUALS(ds->get().key, 100);

			std::ostringstream s;
			s << ds->get();
			TS_TRACE(s.str());
		}
	}


};



#endif