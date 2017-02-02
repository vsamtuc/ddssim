
#include <cxxtest/TestSuite.h>

#include "mathlib.hh"

using namespace dds;
using namespace std;

class MathTestSuite : public CxxTest::TestSuite
{
public:

	void test_estimate_error_observer() 
	{
		estimate_error_observer eerr(4);

		for(int i=1;i<30;i++) {
			double x = i;
			double y = i + (i%5)*0.2;
			eerr.observe(x,y);
		}

		TS_ASSERT_DELTA( mean(eerr.tally), 0.045, 1e-3 );
		TS_ASSERT_DELTA( variance(eerr.tally), 0.002, 1e-3 );
		TS_ASSERT_DELTA( rolling_mean(eerr.tally), 0.017, 1e-3 );
		TS_ASSERT_DELTA( rolling_variance(eerr.tally), 0.0, 1e-3 );
	}	

};

