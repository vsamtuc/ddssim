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




};



#endif