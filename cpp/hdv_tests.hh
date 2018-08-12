
#include <cxxtest/TestSuite.h>

#include "hdv.hh"
#include "binc.hh"

using namespace std;
using namespace hdv;


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
		
		double ddot = dot_inc(old_dot, dx);

		TS_ASSERT_EQUALS(ddot , new_dot);
	}


	void test_dot_product() 
	{
		using binc::print;

		// create some big vectors
		Vec X(1000);
		Vec Y(1000);
		std::iota(std::begin(X), std::end(X), 0.0);
		Y = sqrt(X);

		// compute the initial dot product
		double xy = dot(X, Y);

		// make a small change in X
		Index index { 2,114, 256 };
		delta_vector DX(index);
		DX.xold = X[index];
		X[index] = Vec { 0.0, 1, -10 };
		DX.xnew = X[index];

		// update the value of dot (X,Y)
		xy = dot_inc(xy, DX, Y);

		// check that all went well!
		TS_ASSERT_DELTA( xy , dot(X,Y), 1E-6 );
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



	void test_mask_true_count()
	{
		Index idx {1,4,6};
		Mask m { true, true, false };

		TS_ASSERT_EQUALS(std::count(begin(m), end(m), true), 2);
	}



};


