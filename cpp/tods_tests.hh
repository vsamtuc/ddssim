
#include <cxxtest/TestSuite.h>

#include "data_source.hh"
#include "tods.hh"
#include "binc.hh"

using namespace dds;
using namespace dds::tods;
using namespace agms;
using namespace binc;

class TodsTestSuite : public CxxTest::TestSuite
{
public:


	void test_node_state()
	{
		projection proj(3,10);
		node_stream_state nss(proj, 0.5, 1);
		sketch dE(proj);

		buffered_dataset dset = make_uniform_dataset(1,1,100000,10);

		for(auto rec : dset) {
			nss.update(rec.key, 1.0);
			dE.update(rec.key, 1.0);
		}

		TS_ASSERT_EQUALS(nss.delta_updates, 10);
		TS_ASSERT_EQUALS(norm_Linf(nss.E), 0.0);
		TS_ASSERT_EQUALS(norm_Linf(nss.dE-dE), 0.0);

		double X = dot(nss.dE);
		TS_ASSERT_EQUALS(nss.norm_X_2, X);
		TS_ASSERT_EQUALS(nss.norm_dE_2, X);

		nss.flush();
		TS_ASSERT_DELTA(nss.norm_X_2, X, 1e-9);
		TS_ASSERT_EQUALS(nss.norm_dE_2, 0.0);

		for(auto rec : dset)
			nss.update(rec.key, 1.0);
		TS_ASSERT_DELTA(nss.norm_X_2, 4.*X, 1E-9);
		TS_ASSERT_DELTA(nss.norm_dE_2, X, 1E-9);
	}


	void test_flush()
	{
		projection proj(3,10);
		node_stream_state nss(proj, 0.5, 1);

		buffered_dataset dset = make_uniform_dataset(1,1,100000,10);

		for(auto rec : dset)
			nss.update(rec.key, 1.0);

		TS_ASSERT_EQUALS(nss.delta_updates, 10);
		TS_ASSERT_EQUALS(norm_Linf(nss.E), 0.0);
		double X = nss.norm_X_2;
		TS_ASSERT_DELTA(nss.norm_X_2, nss.norm_dE_2, 1E-9);

		nss.flush();
		TS_ASSERT_EQUALS(nss.delta_updates, 0);
		TS_ASSERT_EQUALS(norm_Linf(nss.dE), 0.0);
		TS_ASSERT_DELTA(nss.norm_X_2, X, 1E-9);
		TS_ASSERT_EQUALS(nss.norm_dE_2, 0.0);
	}

	

};