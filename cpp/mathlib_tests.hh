
#include <cxxtest/TestSuite.h>

#include "mathlib.hh"

using namespace dds;
using namespace std;



/**
	A vector update describes a vector update operation.
  */
struct vector_update
{
	Index index;	/// Positions to update
	Vec delta;		/// Values to add at index

	vector_update() {}
	vector_update(size_t n) : index(n), delta(n) {}

	vector_update(const Index& I, const Vec& D) : index(I), delta(D) { }
	vector_update(Index&& I, const Vec& D) : index(I), delta(D) { }
	vector_update(const Index& I, Vec&& D) : index(I), delta(D) { }
	vector_update(Index&& I, Vec&& D) : index(I), delta(D) { }
};





/**
	A delta vector describes old and new values of
	a vector view, after an update is applied.
 */
struct delta_vector
{
	Index index;
	Vec xold, xnew;

	delta_vector(size_t n) : index(n), xold(n), xnew(n) {}
	delta_vector(const Index& i) : index(i), xold(index.size()), xnew(index.size()) {}
	delta_vector(Index&& i) : index(i), xold(index.size()), xnew(index.size()) {}
	delta_vector(const delta_vector&)=default;
	delta_vector(delta_vector&&)=default;
	delta_vector& operator=(const delta_vector&)=default;
	delta_vector& operator=(delta_vector&&)=default;
};




/**
  	Add to a vector returning a delta.
  */
delta_vector operator+=(Vec& x, vector_update& dvec)
{
	delta_vector dx(dvec.index.size());
	dx.index = dvec.index;
	dx.xold = x[dvec.index];

	x[dvec.index] += dvec.delta;

	dx.xnew = x[dvec.index];

	return dx;
}


double dot(double oldval, const delta_vector& dx, const Vec& y)
{
	return oldval + ((dx.xnew -  dx.xold)*y[dx.index]).sum() ;
}


double dot(double oldval, const delta_vector& dx)
{
	return oldval + dot(dx.xnew) -  dot(dx.xold) ;
}



class MathTestSuite : public CxxTest::TestSuite
{
public:


	void test_deltas()
	{
		Vec x {1.0,2,3,4,5};

		vector_update upd(Index{2,4}, Vec{10.,10.});

		double old_dot = dot(x);
		
		delta_vector dx = (x+=upd) ;
		
		double new_dot = dot(x);
		
		double ddot = dot(old_dot, dx);

		TS_ASSERT_EQUALS(ddot , new_dot);
	}



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


