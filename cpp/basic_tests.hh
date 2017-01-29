
#include <iostream>
#include <boost/numeric/ublas/vector.hpp>
#include <boost/numeric/ublas/vector_sparse.hpp>
#include <boost/numeric/ublas/vector_proxy.hpp>
#include <boost/numeric/ublas/io.hpp>
#include <boost/numeric/ublas/storage.hpp>

#include "dds.hh"

#include <cxxtest/TestSuite.h>



using namespace boost::numeric::ublas;
using namespace std;

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

		indirect_array<> idx(3);
		idx(0) = 1; idx(1) = 5; idx(2)= 1000;
		vector_indirect<mvec> foo(w, idx);

		TS_ASSERT_EQUALS(std::distance(foo.begin(), foo.end()), 3);

		mvec q(2147483647);
		TS_ASSERT_EQUALS(std::distance(q.begin(), q.end()),0);
		q(4) += 1.0;
		TS_ASSERT_EQUALS(std::distance(q.begin(), q.end()),1);
	}


};

