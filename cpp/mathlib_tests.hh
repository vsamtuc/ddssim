
#include <cxxtest/TestSuite.h>

#include "mathlib.hh"

using namespace dds;
using namespace std;




class MathTestSuite : public CxxTest::TestSuite
{
public:



	void test_order_select()
	{
		Vec v { 3, 1, 2};
		Vec u = v;
		TS_ASSERT_EQUALS( order_select(1,u), 2);
		TS_ASSERT( (v == u).min() );

		TS_ASSERT_EQUALS( order_select(0,u), 1);
		TS_ASSERT( (v == u).min() );


		TS_ASSERT_EQUALS( order_select(2,u), 3);
		TS_ASSERT( (v == u).min() );

		TS_ASSERT_THROWS( order_select(3,v), std::length_error );
	}

	void test_median()
	{
		Vec v { 5, 5, 1, 1 };
		TS_ASSERT_EQUALS(median(v), 3.0);

		v = Vec{ 3 };
		TS_ASSERT_EQUALS(v.size(), 1);
		TS_ASSERT_EQUALS(median(v), 3.0);

	}

	void test_estimate_error_observer() 
	{
		estimate_error_observer eerr(4);

		for(int i=1;i<30;i++) {
			double x = i;
			double y = i + (i%5)*0.2;
			eerr.observe(y,x);
		}

		TS_ASSERT_DELTA( mean(eerr.tally), 0.045, 1e-3 );
		TS_ASSERT_DELTA( variance(eerr.tally), 0.002, 1e-3 );
		TS_ASSERT_DELTA( rolling_mean(eerr.tally), 0.017, 1e-3 );
		TS_ASSERT_DELTA( rolling_variance(eerr.tally), 0.0, 1e-3 );
	}	

};


