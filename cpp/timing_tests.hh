#ifndef __TIMING_TESTS_HH__
#define __TIMING_TESTS_HH__

#include <unordered_set>
#include <set>
#include <algorithm>
#include <random>
#include <sstream>
#include <vector>
#include <numeric>
#include <iomanip>

#include <boost/timer/timer.hpp>

#include "data_source.hh"
#include "safezone.hh"
#include "binc.hh"

#include <cxxtest/TestSuite.h>

using namespace std;
using namespace dds;
using namespace agms;
using namespace hdv;
using namespace gm; 

using binc::print;
using binc::sprint;

class TimingTestSuite : public CxxTest::TestSuite
{
public:


	void test_twoway_join_agms_safezone()
	{
		projection proj(7,500);

		size_t D = proj.size();
		Vec E = uniform_random_vector(2*D, -10, 20);
		Vec E1 = E[slice(0,D,1)];
		Vec E2 = E[slice(D,D,1)];

		double E1E2 = dot_est(proj(E1), proj(E2));
		double Tlow = E1E2-0.1*fabs(E1E2);
		double Thigh = E1E2+0.1*fabs(E1E2);

		twoway_join_agms_safezone zeta(E, proj, Tlow, Thigh, true);

		buffered_dataset dset = make_uniform_dataset(2,1,100000,100000);

		delta_vector dX(proj.depth());

		Vec S = E;

		auto e1 = begin(S);
		auto e2 = e1+D;
		auto e3 = e2+D;
		TS_ASSERT_EQUALS(e3, end(S));

		Vec_sketch_view X[2] = { proj(e1,e2), proj(e2,e3) };
		twoway_join_agms_safezone::incremental_state inc;

		double zeta_E = zeta.with_inc(inc, S);
		TS_ASSERT_LESS_THAN_EQUALS(0.0, zeta_E );
		TS_ASSERT_EQUALS(zeta(E), zeta_E);

		{
			boost::timer::auto_cpu_timer t;
			print("Timing from-scratch:");
			for(auto&& rec : dset) {
				TS_ASSERT(rec.sid==1 || rec.sid==2);
				X[rec.sid-1].update(dX, rec.key, rec.upd);
				if(rec.sid==2) dX.index += D;

				zeta(S);
			}
		}		

		{
			boost::timer::auto_cpu_timer t;
			print("Timing incremental:");
			for(auto&& rec : dset) {
				TS_ASSERT(rec.sid==1 || rec.sid==2);
				X[rec.sid-1].update(dX, rec.key, rec.upd);
				if(rec.sid==2) dX.index += D;

				zeta.inc(inc, dX);
			}
		}

	}



};



#endif