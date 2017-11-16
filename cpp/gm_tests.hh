#ifndef __GM_TESTS_HH__
#define __GM_TESTS_HH__

#include <cstdlib>
#include <iostream>
#include <stdexcept>

#include "data_source.hh"
#include "fgm.hh"
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

		fgm::network<qtype::SELFJOIN> net("foo",0, projection(5, 400), 0.5);

		TS_ASSERT_EQUALS( net.sites.size(), 10);
		TS_ASSERT_EQUALS( net.hub->k, 10);

		for(auto p : net.sites) {
			fgm::node_proxy<qtype::SELFJOIN> *np = & net.hub->proxy[p];
			TS_ASSERT_EQUALS(np->proc(), p);
			TS_ASSERT_EQUALS(np->_r_owner, net.hub);
			TS_ASSERT_EQUALS( net.hub->node_ptr[net.hub->node_index[p]] , p);
		}

		for(auto n : net.sites) {
			TS_ASSERT_EQUALS(n->coord._r_owner, n);
			TS_ASSERT_EQUALS(n->coord.proc(), net.hub);
		}
	}

	//
	//  N.B. This test is broken
	// 
	void notest_safezone()
	{
		projection proj(5,400);

		CTX.clear();
		CTX.data_feed(uniform_datasrc(1, 10, 1000, 1000));
		fgm::network<qtype::SELFJOIN> net("foo", 0, proj, 0.5);

		net.hub->start_round();

		isketch W(proj);
		(Vec&)W = uniform_random_vector(proj.size(), -2, 2);

		isketch V(proj);
		V = W;
		W.update(1341234);
		double w = net.hub->query.safe_zone(W);

		for(auto _s : net.sites) {
			safezone& sz = _s->szone;

			TS_ASSERT(sz.valid());
			TS_ASSERT_EQUALS(sz.szone, & net.hub->query.safe_zone);
			TS_ASSERT_EQUALS(sz.Eglobal, & net.hub->query.E);
			double zeta_l, zeta_u;
			sz.prepare_inc(V, zeta_l, zeta_u);
			TS_ASSERT_EQUALS(sz(W, zeta_l, zeta_u), w);
			TS_ASSERT_EQUALS(sz.zeta_E, net.hub->query.zeta_E);
		}
	}


};



#endif
