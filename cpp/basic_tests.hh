
#include <iostream>
#include <tuple>
#include <string>
#include <cstdio>
#include <valarray>

#include <boost/numeric/ublas/vector.hpp>
#include <boost/numeric/ublas/vector_sparse.hpp>
#include <boost/numeric/ublas/vector_proxy.hpp>
#include <boost/numeric/ublas/io.hpp>
#include <boost/numeric/ublas/storage.hpp>

#include "dds.hh"
#include "output.hh"

#include <cxxtest/TestSuite.h>

using namespace boost::numeric::ublas;
using namespace std;
using namespace dds;


class MiscTestSuite : public CxxTest::TestSuite
{
public:


	void test_sizes()
	{
		TS_ASSERT_EQUALS(sizeof(dds::dds_record), 16);

		uint32_t x = 3000000000;
		int64_t xx = x;

		TS_ASSERT_EQUALS(xx, x);
		TS_ASSERT(x>0);
		TS_ASSERT(xx>0);

	}

    void testAddition(void)
    {
        TS_ASSERT(1 + 1 > 1);
        TS_ASSERT_EQUALS(1 + 1, 2);
    }


	void test_mapped_vector()
	{
		namespace u = boost::numeric::ublas;

		using mvec = mapped_vector<double>;

		mvec v(3000);
		v(1)=-2.3;

		mvec w(3000);
		w(5) = 3.14;
		w(1000) = 33.2;

		zero_vector<double> z(3000);

		auto ww = z+w;

		auto vr = subrange(z+w, 0, 500); 

		TS_ASSERT_EQUALS( std::distance(w.begin(), w.end()), 2);

		u::indirect_array<> idx(3);
		idx(0) = 1; idx(1) = 5; idx(2)= 1000;
		vector_indirect<mvec> foo(w, idx);

		TS_ASSERT_EQUALS(std::distance(foo.begin(), foo.end()), 3);

		mvec q(2147483647);
		TS_ASSERT_EQUALS(std::distance(q.begin(), q.end()),0);
		q(4) += 1.0;
		TS_ASSERT_EQUALS(std::distance(q.begin(), q.end()),1);
	}


	void test_ublas_resize() 
	{
		namespace u = boost::numeric::ublas;
		u::vector<double> x;

		TS_ASSERT_EQUALS( x.size(), 0);
		
		x.resize(100);
		TS_ASSERT_EQUALS( x.size(), 100);
		x(99) = 10.2;

		x.resize(12);
		TS_ASSERT_EQUALS( x.size(), 12);
	}

	void test_valarray()
	{
		using std::valarray;

		typedef valarray<double> vec;
		typedef valarray<size_t> idx;

		vec x(300);

		TS_ASSERT_EQUALS(x.size(), 300);

		x = 0;

		idx I = { 2, 57, 43, 33 };
		vec y = { 3., 2., -4, 0 };
		TS_ASSERT_EQUALS( y.size(), 4);
		x[I] = y;

		vec z(4);
		z = x[I];

		std::sort(begin(x), end(x));
		TS_ASSERT(x[0] == -4.);
		TS_ASSERT(x[299] == 3.);
		TS_ASSERT( (x*x).sum() == (y*y).sum() );
	}

	void test_query()
	{
		TS_ASSERT( self_join(3).type == qtype::SELFJOIN );
		TS_ASSERT_EQUALS( self_join(3).param , 3 );

		auto q = self_join(2);
		TS_ASSERT( q == self_join(2) );
		TS_ASSERT( q != self_join(1) );

		TS_ASSERT( q != join(1,2) );
		TS_ASSERT( join(1,2)==join(1,2) );
		TS_ASSERT( join(1,2)!=join(2,1) );

		q.param = 1;
		TS_ASSERT( q != self_join(2) );
		TS_ASSERT( q == self_join(1) );
	}


	void test_tuple()
	{
		using std::tuple;
		using std::string;
		using std::get;

		tuple< int, string > t(3, "haha");

		get<0>(t) = 2;

		TS_ASSERT_EQUALS( get<0>(t), 2 );
		TS_ASSERT_EQUALS( get<1>(t), string("haha"));

		auto t2 = tuple_cat(t,t);
		TS_ASSERT_EQUALS( std::tuple_size<decltype(t2)>::value, 4);
	}



};

