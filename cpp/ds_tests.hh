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
		datasrc ds = generated_ds(initrec, [](dds::dds_record rec)->bool{
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
	}


	size_t ds_length(datasrc ds) {
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
		}
		{
			auto ds = generated_ds(dds_record::zero, max_length(20));
			TS_ASSERT_EQUALS(ds_length(ds) , 20);			
		}
		{
			auto F = FSEQ | max_length(10) | max_length(20);
			auto ds = generated_ds(dds_record::zero, F);
			TS_ASSERT_EQUALS(ds_length(ds) , 10);
		}
	}

	template <typename KGen, typename TSGen>
	static datasrc make_data_source(mt19937& rng, 
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
	}

	void test_buffered()
	{
		mt19937 rng(12344);
		std::uniform_int_distribution<dds::key_type> key_gen(50,100);
		std::uniform_int_distribution<dds::timestamp> ts_gen(2,10);

		shared_ptr<data_source> mds { make_data_source(rng, key_gen, ts_gen) };
		buffered_dataset dset;
		dset.load(mds);

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

	void test_uniform()
	{
		const stream_id maxstream = 10;
		const source_id maxsource = 20;
		const key_type maxkey = 1000000;
		const timestamp maxtime = 10000;

		datasrc ds { new uniform_data_source(maxstream, maxsource, maxkey, maxtime) };

		buffered_dataset dset;
		dset.load(ds);
		ds_metadata m;
		dset.analyze(m);

		// check data
		TS_ASSERT_EQUALS( dset.size(), maxtime );
		TS_ASSERT_EQUALS( m.size(), maxtime );
		TS_ASSERT_EQUALS(1, m.mintime());
		TS_ASSERT_EQUALS(m.maxtime(), maxtime);
		TS_ASSERT_LESS_THAN_EQUALS(1, m.minkey());
		TS_ASSERT_LESS_THAN_EQUALS(m.maxkey(), maxkey);

		for(auto sid: m.stream_ids()) {
			TS_ASSERT_LESS_THAN_EQUALS(1, sid);
			TS_ASSERT_LESS_THAN_EQUALS(sid, maxstream);
		}

		for(auto hid: m.source_ids()) {
			TS_ASSERT_LESS_THAN_EQUALS(1, hid);
			TS_ASSERT_LESS_THAN_EQUALS(hid, maxsource);
		}

		// check stream metadata against dataset metadata
		ds_metadata dsm = static_pointer_cast<uniform_data_source>(ds)
							->metadata();
		TS_ASSERT_EQUALS(dsm.size(), m.size());
		TS_ASSERT_EQUALS(dsm.mintime(), m.mintime());
		TS_ASSERT_EQUALS(dsm.maxtime(), m.maxtime());
		TS_ASSERT_LESS_THAN_EQUALS(dsm.minkey(), m.minkey());
		TS_ASSERT_LESS_THAN_EQUALS(m.maxkey(), dsm.maxkey());

		TS_ASSERT_EQUALS( dsm.stream_ids().size(), maxstream);
		TS_ASSERT( all_of(dsm.stream_ids().begin(), dsm.stream_ids().end(), 
			[=](auto s) { return 1<=s && s<= maxstream; }));

		TS_ASSERT_EQUALS( dsm.source_ids().size(), maxsource);
		TS_ASSERT( all_of(dsm.source_ids().begin(), dsm.source_ids().end(), 
			[=](auto s) { return 1<=s && s<= maxsource; }));

		TS_ASSERT( all_of(dset.begin(), dset.end(), 
			[](auto rec){return rec.sop==INSERT;}));
	}

};



#endif