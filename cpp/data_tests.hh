#ifndef __DATA_TESTS_HH__
#define __DATA_TESTS_HH__

#include <cxxtest/TestSuite.h>
#include <unordered_set>
#include <set>
#include <algorithm>
#include <random>
#include <sstream>
#include <vector>
#include <numeric>
#include <iomanip>

#include "data_source.hh"
#include "output.hh"
#include "agms.hh"

using std::unordered_set;
using std::set;
using std::vector;
using std::min_element;
using std::max_element;
using std::mt19937;

using namespace dds;
using namespace agms;
using namespace hdv;
using namespace tables;


mt19937 rng(123122);

ostream& operator<<(ostream& s, const Vec& v)
{
	s << "[";
	for(auto x:v) 
		s << x << ",";
	return s << "]";
}

class DatasetTestSuite : public CxxTest::TestSuite
{
public:

	typedef vector<key_type> Stream;
	typedef frequency_vector<key_type> FVec;

	Stream make_stream(key_type maxkey, size_t length)
	{
		std::uniform_int_distribution<key_type> U(1,maxkey);
		Stream s;
		s.reserve(length);
		for(size_t i=0;i<length;i++) {
			auto X = U(rng);
			s.push_back(X);
		}
		return s;
	}

	FVec make_sparse(const Stream& S)
	{
		FVec ret;
		for(key_type x : S) {
			ret[x] += 1;
		}
		return ret;
	}

	sketch make_sketch(projection proj, const FVec& f)
	{
		sketch sk(proj);
		size_t count = 0;
		for(auto x=begin(f);x!=end(f);x++) {
			sk.update(x->first, x->second);
			count ++;
		}
		// cout << "FVec has " << count << " distinct keys" << endl;
		// cout << "sketch dot = " << dot_estvec(sk,sk) << endl;
		return sk;
	}


	bool success_selfjoin(const projection& proj, 
		key_type maxkey, size_t length)
	{
		Stream S = make_stream(maxkey, length);
		FVec f = make_sparse(S);
		sketch sk = make_sketch(proj, f);

		double exc = inner_product(f,f);
		double est = dot_est(sk,sk);
		double err = fabs((exc-est)/exc);
		 // cout << "Estimated =" << est 
		 // 	<< "  exact = " << exc 
		 // 	<< "  error = " << err 
		 // 	<< "  theoretical = " << proj.epsilon()
		 // 	<< endl;
		return err<proj.epsilon();
	}

	bool selfjoin_agms_accuracy(
		depth_type D, size_t L,
		size_t N, 
		key_type maxkey, size_t length)
	{
		projection proj(D, L);
		proj.set_epsilon(proj.ams_epsilon()/3.);

		size_t fails = 0;

		ostringstream msgstream;
		msgstream
			<< setw(3) << D 
			<< setw(6) << L
			<< setw(9) << maxkey
			<< setw(9) << length
			<< " :";
		progress_bar pb(stdout, 20, msgstream.str());
		pb.start(N);
		for(size_t i=0;i<N;i++) {
			if(!success_selfjoin(proj, maxkey, length)) {
				fails ++;
			}
			pb.tick();
		}
		pb.finish();

		double Prfail = (double)fails/ N ;

		cout << "Prob[fail]=" << Prfail << "(" << fails << ")"
			<< "   theoretical=" << proj.prob_failure() 
			<< endl;
		return Prfail <= proj.prob_failure();
	}


	void test_selfjoin_agms()
	{
		cout << endl;
		for(depth_type D=3; D<=7; D+=2)
		for(size_t L=500; L<=2500; L+=500)
		for(size_t mk = 1000; mk<=100000; mk*=10) {			
			for(size_t len=1000; len <=100000; len*=10) 
			{
				TS_ASSERT(selfjoin_agms_accuracy(D, L, 100, mk, len));
			}
		}
	}
	

	void test_crawdad()
	{
		datasrc cdad = crawdad_ds("/home/vsam/src/datasets/wifi_crawdad_sorted");
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
			TS_ASSERT_EQUALS(1,  ddsrec.upd);

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
	}


	void test_wcup()
	{
		datasrc wcup_orig = wcup_ds("/home/vsam/src/datasets/wc_day44_1");
		buffered_dataset wcup_dset;
		wcup_dset.load(wcup_orig);
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
			TS_ASSERT_EQUALS(1, ddsrec.upd);

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

	}


};

#endif