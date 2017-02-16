#ifndef __SZONE_TESTS__
#define __SZONE_TESTS__

#include <cstdlib>
#include <iostream>
#include <chrono>

#include "data_source.hh"
#include "safezone.hh"

#include <cxxtest/TestSuite.h>

using namespace std;
using namespace dds;
using namespace agms;


class SZoneTestSuite : public CxxTest::TestSuite
{
public:


	/*
		Testing construction and properties of the quantile
		safezones
		*/
	void test_quantile()
	{	

		Vec zE = { 13.0, 17, 26, 31, 52 };

		quantile_safezone qsz(zE);
		TS_ASSERT_EQUALS(qsz.L.size(), 5);

		quantile_safezone_non_eikonal szne(std::move(qsz),3);
		TS_ASSERT_EQUALS(qsz.L.size(), 0);
		TS_ASSERT_EQUALS(szne.n, 5);
		TS_ASSERT_EQUALS(szne.k, 3);

		Vec zX1 = { -3., 11, 8, -1, -2  };
		TS_ASSERT_EQUALS( szne(zX1) , -174.0 );
	}


	/*
		Testing function of the quantile
		safezones, eikonal and non-eikonal
		*/
	void test_quantile_est()
	{

		Vec zE = { 13.0, 17, 26, 11, -33, 31, 52 };

		// Test that the eikonal and non-eikonal safe zones are equal

		quantile_safezone_non_eikonal szne(zE, (zE.size()+1)/2);
		TS_ASSERT_EQUALS(szne.n, 7);
		TS_ASSERT_EQUALS(szne.k, 4);
		TS_ASSERT_EQUALS(szne.L.size(), 6);

		quantile_safezone_eikonal sze(zE, (zE.size()+1)/2);
		TS_ASSERT_EQUALS(szne.n, 7);
		TS_ASSERT_EQUALS(szne.k, 4);
		TS_ASSERT_EQUALS(szne.L.size(), 6);


		Vec zX(zE.size());
		for(size_t i=0;i<10000;i++) {
			// fill a random vector
			for(size_t j=0;j<zX.size();j++)
				zX[j] = random()%100 - 50;
				
			double we = sze(zX);
			double wne = szne(zX);

			TS_ASSERT( (we>=0) == (wne>=0));
		}

	}




};






#endif