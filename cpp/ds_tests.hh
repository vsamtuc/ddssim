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

	size_t ds_length(datasrc ds) {
		size_t count = 0;
		while(ds->valid()) {
			count++;
			ds->advance();
		}
		return count;		
	}


	void test_buffered()
	{
		mt19937 rng(12344);
		std::uniform_int_distribution<dds::key_type> key_gen(50,100);
		std::uniform_int_distribution<dds::timestamp> ts_gen(2,10);

		datasrc mds = uniform_datasrc(1, 1, 100, 10);
		buffered_dataset dset;
		dset.load(mds);

		datasrc ds { new buffered_data_source(dset) };

		for(; ds->valid(); ds->advance()) {
			TS_ASSERT_EQUALS(ds->get().sid, 1);
			TS_ASSERT_LESS_THAN_EQUALS(1, ds->get().ts);
			TS_ASSERT_LESS_THAN_EQUALS(1, ds->get().key);
			TS_ASSERT_LESS_THAN_EQUALS(ds->get().key, 100);

		}
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
			[](auto rec){return rec.upd==1;}));
	}


	void test_uniform_rewind()
	{
		const stream_id maxstream = 10;
		const source_id maxsource = 20;
		const key_type maxkey = 1000000;
		const timestamp maxtime = 10000;

		datasrc ds { new uniform_data_source(maxstream, maxsource, maxkey, maxtime) };

		buffered_dataset dset1, dset2;
		dset1.load(ds);
		ds->rewind();
		dset2.load(ds);

		TS_ASSERT_EQUALS( dset1, dset2 );
		TS_ASSERT_EQUALS( dset1[0].ts, 1);
		TS_ASSERT_EQUALS( dset2[0].ts, 1);

		TS_ASSERT_EQUALS( dset1.size(), maxtime);

		TS_ASSERT_EQUALS( dset1[maxtime-1].ts, maxtime);
		TS_ASSERT_EQUALS( dset2[maxtime-1].ts, maxtime);

	}

	void test_loops()
	{
		const stream_id maxstream = 10;
		const source_id maxsource = 20;
		const key_type maxkey = 1000000;
		const timestamp maxtime = 100;
		const size_t loops = 10;

		datasrc ds { new uniform_data_source(maxstream, maxsource, maxkey, maxtime) };

		datasrc lds = looped_ds(ds, loops);

		buffered_dataset dset;
		dset.load(lds);
		ds_metadata dsm;
		dset.analyze(dsm);

		TS_ASSERT_EQUALS(dsm.size(), loops*maxtime );
		TS_ASSERT_EQUALS(dsm.mintime(), 1);
		TS_ASSERT_EQUALS(dsm.maxtime(), loops*maxtime);

		lds->rewind();
		buffered_dataset dset2;
		dset2.load(lds);
		TS_ASSERT_EQUALS(dset, dset2);
	}

};



#endif