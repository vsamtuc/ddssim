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

using namespace dds;

class DataSourceTestSuite : public CxxTest::TestSuite
{
public:

	void test_generated()
	{
		dds::dds_record initrec { 0, 0u, dds::INSERT, 0, 0 };
		data_source* ds = generated_ds(initrec, [](dds::dds_record rec)->bool{
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
		using dds::dds_record;
		{
			auto ds = generated_ds(dds_record::zero, max_length(10));
			TS_ASSERT_EQUALS(ds_length(ds) , 10);
			delete ds;
		}
		{
			auto ds = generated_ds(dds_record::zero, max_length(20));
			TS_ASSERT_EQUALS(ds_length(ds) , 20);			
			delete ds;
		}
		{
			auto F = FSEQ | max_length(10) | max_length(20);
			auto ds = generated_ds(dds_record::zero, F);
			TS_ASSERT_EQUALS(ds_length(ds) , 10);
			delete ds;
		}
	}

	template <typename KGen, typename TSGen>
	static data_source* make_data_source(mt19937& rng, 
		KGen& key_gen, TSGen& ts_gen) 
	{
		auto F = FSEQ | max_length(10) 
			| set_attr(& dds::dds_record::key, rng, key_gen)
			| set_attr(& dds::dds_record::sid, (stream_id)17)
			| addto_attr(& dds::dds_record::hid, (source_id)1)
			| set_attr(& dds::dds_record::sop, dds::INSERT)
			| addto_attr(& dds::dds_record::ts, rng, ts_gen)
			;

		dds::dds_record r0 = dds::dds_record::zero;
		r0.ts = 140;
		auto gends = generated_ds(r0, F);
		return time_window(gends, 15);

	}

	void test_func_set_attr()
	{
		mt19937 rng(12344);
		std::uniform_int_distribution<dds::key_type> key_gen(50,100);
		std::uniform_int_distribution<dds::timestamp> ts_gen(2,10);

		auto ds = make_data_source(rng, key_gen, ts_gen);
		for(; ds->valid(); ds->advance()) {
			TS_ASSERT_EQUALS(ds->get().sid, 17);
			TS_ASSERT_LESS_THAN_EQUALS(40, ds->get().ts);
			TS_ASSERT_LESS_THAN_EQUALS(50, ds->get().key);
			TS_ASSERT_LESS_THAN_EQUALS(ds->get().key, 100);

			std::ostringstream s;
			s << ds->get();
			TS_TRACE(s.str());
		}
		delete ds;
	}

	void test_buffered()
	{
		mt19937 rng(12344);
		std::uniform_int_distribution<dds::key_type> key_gen(50,100);
		std::uniform_int_distribution<dds::timestamp> ts_gen(2,10);

		data_source* mds = make_data_source(rng, key_gen, ts_gen);
		buffered_dataset dset;
		dset.consume(mds);

		auto ds = new buffered_data_source(dset);
		for(; ds->valid(); ds->advance()) {
			TS_ASSERT_EQUALS(ds->get().sid, 17);
			TS_ASSERT_LESS_THAN_EQUALS(40, ds->get().ts);
			TS_ASSERT_LESS_THAN_EQUALS(50, ds->get().key);
			TS_ASSERT_LESS_THAN_EQUALS(ds->get().key, 100);

			std::ostringstream s;
			s << ds->get();
			TS_TRACE(s.str());
		}
		delete ds;
	}

};



#endif