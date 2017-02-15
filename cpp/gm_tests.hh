#ifndef __GM_TESTS_HH__
#define __GM_TESTS_HH__

#include <cstdlib>
#include <iostream>

#include "data_source.hh"
#include "geometric.hh"

#include <cxxtest/TestSuite.h>

using namespace std;
using namespace dds;
using namespace dds::gm;


class GeomTestSuite : public CxxTest::TestSuite
{
public:

	void test_mixing()
	{
		// prepare the context
		const stream_id Nstream = 3;
		const source_id k = 8;
		CTX.data_feed(new uniform_data_source(Nstream,k,1000,1000));

		projection proj(5, 100);
		gm::network *nw = new gm::network(proj);

		TS_ASSERT_EQUALS(nw->size(), k+1);
		TS_ASSERT_EQUALS(nw->nodes.size(), k);

		for(auto i : nw->nodes) {
			node* n = i.second;
			TS_ASSERT_EQUALS( n->site_id(), i.first );

			TS_ASSERT_EQUALS( n->__local_stream_state.size(), Nstream);
			for(stream_id sid : CTX.metadata().stream_ids()) {
				TS_ASSERT_EQUALS( & n->local_stream_state(sid) , & n->drift_vector(sid) );
			}

		}

	}


	void test_quantile()
	{	
		using namespace dds::gm2;

		Vec zE = { 13.0, 17, 26, 31, 52 };

		quantile_safezone qsz(zE);
		TS_ASSERT_EQUALS(qsz.L.size(), 5);

		quantile_safezone_non_eikonal szne(std::move(qsz),3);
		TS_ASSERT_EQUALS(qsz.L.size(), 0);
		TS_ASSERT_EQUALS(szne.n, 5);
		TS_ASSERT_EQUALS(szne.k, 3);

		Vec zX1 = { -3., 11, 8, -1, -2  };
		TS_ASSERT_EQUALS( szne(zX1) , -174.0 );
	}

	void test_quantile_est()
	{
		using namespace dds::gm2;

		Vec zE = { 13.0, 17, 26, 11, -33, 31, 52 };

		// Test that the eikonal and non-eikonal safe zones are equal

		quantile_safezone_non_eikonal szne(zE, (zE.size()+1)/2);
		TS_ASSERT_EQUALS(szne.n, 7);
		TS_ASSERT_EQUALS(szne.k, 4);
		TS_ASSERT_EQUALS(szne.L.size(), 6);

		quantile_safezone_eikonal sze(zE, (zE.size()+1)/2);
		TS_ASSERT_EQUALS(szne.n, 7);
		TS_ASSERT_EQUALS(szne.k, 4);
		TS_ASSERT_EQUALS(szne.L.size(), 6);


		Vec zX(zE.size());
		for(size_t i=0;i<10000;i++) {
			// fill a random vector
			for(size_t j=0;j<zX.size();j++)
				zX[j] = random()%100 - 50;
				
			double we = sze(zX);
			double wne = szne(zX);

			TS_ASSERT( (we>=0) == (wne>=0));
		}

	}


};



#endif