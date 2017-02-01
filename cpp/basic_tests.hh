
#include <iostream>
#include <tuple>
#include <string>
#include <cstdio>

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

	struct silly_table : output_table
	{
		silly_table(const char* n) 
		: output_table(n, {&count, &mean_x, &label}) {}

		column<int> count {"count", "%d"};
		column<double> mean_x {"mean_x", "%f" };
		column<std::string> label {"label", "%s" };
	};

	void test_output()
	{
		silly_table tab("SILLY");

		tab.count = 1;
		tab.mean_x = 3.14;
		tab.label = "Hello";

		char* buf = NULL;
		size_t len = 0;
		FILE* f = open_memstream(&buf, &len);

		tab.emit_header(f);
		tab.emit(f);
		tab.emit(f);
		tab.emit(f);

		fclose(f);

		const char* expected =
		"#INDEX,count,mean_x,label\n"
		"SILLY,1,3.140000,Hello\n"
		"SILLY,1,3.140000,Hello\n"
		"SILLY,1,3.140000,Hello\n";

		TS_ASSERT_EQUALS( buf, expected );
		free(buf);
	}

	void test_time_series()
	{
		time_series T("series");

		column<double> t1 { "t1", "%f" };
		column<double> t2 { "t2", "%f" };

		T.add(t1);
		T.add(t2);

		//T.emit_header(stdout);

		t1 = 13.2;
		t2 = 11.4;
		for(dds::timestamp t=10; t < 12; t++) {
			T.now = t;
			t1 = t1.value() + t;
			t2 = t2.value() - t;
			//T.emit(stdout);
		}


	}

};

