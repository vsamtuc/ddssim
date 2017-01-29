#ifndef __DATA_TESTS_HH__
#define __DATA_TESTS_HH__

#include <cxxtest/TestSuite.h>
#include <unordered_set>
#include <set>
#include <algorithm>
#include <random>
#include <sstream>
#include "data_source.hh"

using std::unordered_set;
using std::set;
using std::min_element;
using std::max_element;
using std::mt19937;

using namespace dds;

class DatasetTestSuite : public CxxTest::TestSuite
{
public:

	void test_crawdad()
	{
		data_source* cdad = crawdad_ds("/home/vsam/src/datasets/wifi_crawdad_sorted");
		size_t count = 0;
		dds::timestamp oldts = -1;

		unordered_set<dds::stream_id> sids;
		unordered_set<dds::source_id> hids;
		unordered_set<dds::key_type> keys;


		for(; *cdad; (*cdad)++) {
			count ++;
			dds::dds_record ddsrec = cdad->get();

			// assert that timestamps are increasing
			TS_ASSERT_LESS_THAN_EQUALS(oldts, ddsrec.ts);
			oldts = ddsrec.ts;

			// assert that values are all positive
			keys.insert(ddsrec.key);
			TS_ASSERT_LESS_THAN_EQUALS(0, ddsrec.key);
			TS_ASSERT_EQUALS(dds::INSERT, ddsrec.sop);

			sids.insert(ddsrec.sid);
			hids.insert(ddsrec.hid);
		}
		TS_ASSERT_EQUALS(count, 1361567);

		TS_ASSERT_EQUALS(keys.size(), 7563);
		auto keymin  = * min_element(keys.begin(), keys.end());
		auto keymax  = *max_element(keys.begin(), keys.end());
		TS_ASSERT_EQUALS(keymin, 0);
		TS_ASSERT_EQUALS(keymax, 255620);

		TS_ASSERT_EQUALS(sids.size(), 2);
		TS_ASSERT_EQUALS(*min_element(sids.begin(), sids.end()), 0);
		TS_ASSERT_EQUALS(*max_element(sids.begin(), sids.end()), 1);

		TS_ASSERT_EQUALS(hids.size(), 27);
		TS_ASSERT_EQUALS(*min_element(hids.begin(), hids.end()), 0);
		TS_ASSERT_EQUALS(*max_element(hids.begin(), hids.end()), 26);
		delete cdad;
	}


	void test_wcup()
	{
		data_source* wcup_orig = wcup_ds("/home/vsam/src/datasets/wc_day44");
		buffered_dataset wcup_dset;
		wcup_dset.consume(wcup_orig);
		buffered_data_source* wcup = new buffered_data_source(wcup_dset);

		size_t count = 0;
		dds::timestamp oldts = -1;

		set<dds::stream_id> sids;
		set<dds::source_id> hids;
		unordered_set<dds::key_type> keys;

		TS_ASSERT( wcup->valid() );

		dds::timestamp tmin = wcup->get().ts;

		for(; *wcup; (*wcup)++) {
			count ++;
			dds::dds_record ddsrec = wcup->get();

			// assert that timestamps are increasing
			TS_ASSERT_LESS_THAN_EQUALS(oldts, ddsrec.ts);
			oldts = ddsrec.ts;

			// assert that values are all positive
			keys.insert(ddsrec.key);
			TS_ASSERT_LESS_THAN_EQUALS(0, ddsrec.key);
			TS_ASSERT_EQUALS(dds::INSERT, ddsrec.sop);

			sids.insert(ddsrec.sid);
			hids.insert(ddsrec.hid);

			std::ostringstream s;
			s << wcup->get();
			TS_TRACE(s.str());
		}
		dds::timestamp tmax = oldts;

		dds::ds_metadata md = wcup->metadata();
		TS_ASSERT_EQUALS(count, 6999999);
		TS_ASSERT_EQUALS(count, md.size());

		TS_ASSERT_EQUALS( tmin, md.mintime() );
		TS_ASSERT_EQUALS( tmax, md.maxtime() );

		TS_ASSERT_EQUALS( sids.size(), md.stream_ids().size() );
		TS_ASSERT( std::equal(sids.begin(), sids.end(), 
			md.stream_ids().begin()) );

		TS_ASSERT_EQUALS( hids.size(), md.source_ids().size() );
		TS_ASSERT( std::equal(hids.begin(), hids.end(), 
			md.source_ids().begin()) );

		delete wcup;
	}


};

#endif