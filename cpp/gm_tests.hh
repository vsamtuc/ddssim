#ifndef __GM_TESTS_HH__
#define __GM_TESTS_HH__

#include <cstdlib>
#include <iostream>
#include <stdexcept>

#include "data_source.hh"
#include "fgm.hh"
#include "gm_query.hh"
#include "binc.hh"

#include <cxxtest/TestSuite.h>

using namespace std;
using namespace dds;
using namespace gm;



class GeomTestSuite : public CxxTest::TestSuite
{
public:


	void test_gm2_network()
	{

		// load a dataset to the coord to initialize the
		// metadata
		CTX.data_feed(uniform_datasrc(1, 10, 1000, 1000));

		auto Q = new agms_continuous_query<selfjoin_query_state>(
			vector<stream_id> { 0 }, 
			projection(5, 400), 0.5, qtype::SELFJOIN, query_config()
			);
		fgm::network net("foo", Q, protocol_config());

		TS_ASSERT_EQUALS( net.sites.size(), 10);
		TS_ASSERT_EQUALS( net.hub->k, 10);

		for(auto p : net.sites) {
			fgm::node_proxy *np = & net.hub->proxy[p];
			TS_ASSERT_EQUALS(np->proc(), p);
			TS_ASSERT_EQUALS(np->_r_owner, net.hub);
			TS_ASSERT_EQUALS( net.hub->node_ptr[net.hub->node_index[p]] , p);
		}

		for(auto n : net.sites) {
			TS_ASSERT_EQUALS(n->coord._r_owner, n);
			TS_ASSERT_EQUALS(n->coord.proc(), net.hub);
		}
	}



};



#endif
