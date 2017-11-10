#ifndef __SZONE_TESTS__
#define __SZONE_TESTS__

#include <cstdlib>
#include <iostream>
#include <chrono>

#include "data_source.hh"
#include "safezone.hh"
#include "binc.hh"

#include <cxxtest/TestSuite.h>

using namespace std;
using namespace dds;
using namespace agms;
using namespace gm; 
using binc::print;
using binc::sprint;

class SZoneTestSuite : public CxxTest::TestSuite
{
public:


	/*
		Testing function of the quantile
		safezones, eikonal and non-eikonal
		*/
	void test_quorun_est()
	{

		Vec zE = { 13.0, 17, 26, 11, -33, 31, 52 };

		// Test that the eikonal and non-eikonal safe zones are equal

		quorum_safezone_fast szne(zE, (zE.size()+1)/2);
		TS_ASSERT_EQUALS(szne.n, 7);
		TS_ASSERT_EQUALS(szne.k, 4);
		TS_ASSERT_EQUALS(szne.L.size(), 6);

		quorum_safezone sze(zE, (zE.size()+1)/2);
		TS_ASSERT_EQUALS(szne.n, 7);
		TS_ASSERT_EQUALS(szne.k, 4);
		TS_ASSERT_EQUALS(szne.L.size(), 6);


		for(size_t i=0;i<10000;i++) {
			// fill a random vector
			Vec zX = uniform_random_vector(zE.size(), -50, 50);
				
			double we = sze(zX);
			double wne = szne(zX);

			TS_ASSERT( (we>=0) == (wne>=0));
		}
	}

	// Test that k=n produces the min function
	void test_quorum_AND_case()
	{
		size_t N=7;

		// run 10 tests
		for(size_t i=0; i<10; i++) {
			// produce a random reference point
			Vec E = uniform_random_vector(N, 0.1, 10);
			quorum_safezone sz(E, N);
			quorum_safezone szf(E, N);

			// test 100 vectors
			for(size_t j=0; j<100; j++) {
				Vec z = uniform_random_vector(N, -20, 20);
				TS_ASSERT_DELTA( sz(z), z.min(), 1E-10 );
				TS_ASSERT_DELTA( sz(z), z.min(), 1E-10 );
			}
		}
	}

	// Test that k=1 produces the OR function
	void test_quorum_OR_case()
	{
		size_t N=7;

		// run 10 tests
		for(size_t i=0; i<10; i++) {
			// produce a random reference point
			Vec E = uniform_random_vector(N, 0.1, 10);
			if(E.max() <= 0) {
				i--; continue;   // discard the sample vector
			}

			quorum_safezone sz(E, 1);
			quorum_safezone_fast szf(E,1);

			double Epnorm = norm_L2( E[E>0.0] );

			// test 100 vectors
			for(size_t j=0; j<100; j++) {
				Vec z = uniform_random_vector(N, -20, 20);
				TS_ASSERT_DELTA( sz(z), ((z*E)[E>0.0]).sum()/Epnorm , 1E-10 );
				TS_ASSERT_DELTA( szf(z), ((z*E)[E>0.0]).sum() , 1E-10 );
			}
		}
	}


	// Test the semijoin upper bound for accuracy.
	// This is done by taking random sketches and comparing
	// to the distance of the admissible region.
	void test_sj_ub()
	{
		projection proj(5, 10);

		// run 10 tests
		for(size_t i=0; i<10; i++) {

			sketch E(proj);
			E = uniform_random_vector(proj.size(), -10, 10);

			double Emed = dot_est(E);

			selfjoin_agms_safezone_upper_bound sz(E, 1.1*Emed);

			TS_ASSERT_LESS_THAN( 0.0, sz(E) );

			// test 100 sketches from each type
			size_t count_inA = 0;
			size_t count_notinA  = 0;
			size_t count_inZ = 0, count_notinZ=0;
			while(count_notinA<100 || count_inA<100)
			{

				sketch X(proj);
				X = E + uniform_random_vector(proj.size(), -5, 5);

				bool inA = dot_est(X) < 1.1*Emed;
				if(inA) count_inA++; else count_notinA++;

				double zeta = sz(X);
				bool inZ = zeta>0;
				if(zeta>0) count_inZ++; else count_notinZ++; 

				TS_ASSERT(  inZ <= inA );
			}
			TS_TRACE("Upper bound test:");
			TS_TRACE(sprint("in A=",count_inA, "not in A=",count_notinA).c_str());
			TS_TRACE(sprint("in Z=",count_inZ, "not in Z=",count_notinZ).c_str());

		}
	}

	// Test the semijoin lower bound for accuracy.
	// This is done by taking random sketches and comparing
	// to the distance of the admissible region.
	void test_sj_lb()
	{
		projection proj(5, 10);

		// run 10 tests
		for(size_t i=0; i<10; i++) {

			sketch E(proj);
			E = uniform_random_vector(proj.size(), -10, 10);

			double Emed = dot_est(E);

			selfjoin_agms_safezone_lower_bound sz(E, 0.9*Emed);

			TS_ASSERT_LESS_THAN( 0.0, sz(E) );

			// test 100 sketches from each type
			size_t count_inA = 0;
			size_t count_notinA  = 0;
			size_t count_inZ = 0, count_notinZ=0;
			while(count_notinA<100 || count_inA<100)
			{

				sketch X(proj);
				X = E + uniform_random_vector(proj.size(), -5, 5);
				bool inA = dot_est(X) >= 0.9*Emed;
				if(inA) count_inA++; else count_notinA++;

				double zeta = sz(X);
				bool inZ = zeta>0;
				if(zeta>0) count_inZ++; else count_notinZ++; 

				TS_ASSERT(  inZ <= inA );
			}
			TS_TRACE("Lower bound test:");
			TS_TRACE(sprint("in A=",count_inA, "not in A=",count_notinA).c_str());
			TS_TRACE(sprint("in Z=",count_inZ, "not in Z=",count_notinZ).c_str());
		}
	}

	// Test the semijoin lower bound for accuracy.
	// This is done by taking random sketches and comparing
	// to the distance of the admissible region.
	void test_sj()
	{
		projection proj(5, 10);

		// run 10 tests
		for(size_t i=0; i<10; i++) {

			sketch E(proj);
			E = uniform_random_vector(proj.size(), -10, 10);

			double Emed = dot_est(E);

			selfjoin_agms_safezone sz(E, 0.9*Emed, 1.1*Emed);
			TS_ASSERT_LESS_THAN( 0.0, sz(E) );

			// test 100 sketches from each type
			size_t count_inA = 0;
			size_t count_notinA  = 0;
			size_t count_inZ = 0, count_notinZ=0;

#define COMPARE_WITH_BOUNDING_BALLS 0
#if COMPARE_WITH_BOUNDING_BALLS
			size_t count_inBB=0;
#endif

			//while(count_notinA<100 || count_inA<100)
			for(size_t j=0;j<1000;j++)
			{

				sketch X(proj);
				//X = E+uniform_random_vector(proj.size(), -2, 2);
				X = E+uniform_random_vector(proj.size(), -1.65, 1.65);
				bool inA = fabs(dot_est(X)-Emed) <= 0.1*Emed;
				if(inA) count_inA++; else count_notinA++;

				double zeta = sz(X);
				bool inZ = zeta>0;
				if(zeta>0) count_inZ++; else count_notinZ++; 

				TS_ASSERT(  inZ <= inA );

#if COMPARE_WITH_BOUNDING_BALLS
				// Comparing to the Covering Spheres
				// Compute distance of (X+E)/2 to the adm. region
				if(!inA) continue;
				bool in_BB=false;

				sketch midpXE = (X+E)/2;

				if(fabs(dot_est(midpXE) - Emed) <= 0.1*Emed) {  // if (X+E)/2 in non-admissible, we need not continue

					// To compute the distance of (X+E)/2 from the admissible region, 
					// construct an eikonal safezone with (X+E)/2 as the reference point,
					// and call it on itself!					
					selfjoin_agms_safezone szmidpXE(midpXE, 0.9*Emed, 1.1*Emed);
					double distX = szmidpXE(midpXE);

					// the distance distX should be greater than sz(midpXE)
					TS_ASSERT_LESS_THAN_EQUALS(sz(midpXE), distX+1E-9); // increase distX by a small epsilon, to avoid rounding errors!

					// ok, compare to half-distance of X and E
					double nXmE = norm_L2(X-E)/2;
					in_BB = distX >= nXmE;
				} 

				if(in_BB) count_inBB++;
				TS_ASSERT(in_BB <= inZ);
#endif
			}
			TS_TRACE("Both bounds test:");
			TS_TRACE(sprint("in A=",count_inA, "not in A=",count_notinA).c_str());
			TS_TRACE(sprint("in Z=",count_inZ, "not in Z=",count_notinZ).c_str());
#if COMPARE_WITH_BOUNDING_BALLS
			TS_TRACE(sprint("in BB=",count_inBB).c_str());
#endif
		}
	}

	

	// test the incremental implementation of semijoin lower bound
	void test_sj_inc()
	{
		projection proj(5, 10);
		sketch E(proj);

		// Make a random dataset of 100 points to init E
		buffered_dataset dset = make_uniform_dataset(1,1,100000,100);
		for(auto rec : dset) {
			E.update(rec.key);
		}
		double Emed = dot_est(E);

		selfjoin_agms_safezone sz(E, 0.8*Emed, 1.2*Emed);

	
		// Repeat 100 times, with different increment sequences, starting
		// from E.
		for(size_t i=0; i<100; i++) {
			isketch X(proj);
			(sketch&)X = E;

			// Make a random dataset of 100 points, describing a path of updates
			dset = make_uniform_dataset(1,1,100000,100);

			// Construct an incremental state on E
			selfjoin_agms_safezone::incremental_state incstate;
			double zeta_E = sz.with_inc(incstate, X);
			TS_ASSERT_DELTA( zeta_E, sz(E), 1.E-9 );
			TS_TRACE(sprint("z_E=",zeta_E).c_str());

			// insert the dataset
			for(auto rec : dset) {
				X.update(rec.key);
				double z_from_scratch = sz(X);
				double z_incremental = sz.inc(incstate, X.delta);
				TS_ASSERT_DELTA( z_from_scratch, z_incremental, 1E-9);
				TS_TRACE(sprint("z=",z_from_scratch));
			}

			// permute the dataset
			std::random_shuffle(dset.begin(), dset.end());

			// remove the dataset
			for(auto rec : dset) {
				X.update(rec.key, -1.0);
				double z_from_scratch = sz(X);
				double z_incremental = sz.inc(incstate, X.delta);
				TS_ASSERT_DELTA( z_from_scratch, z_incremental, 1E-9);
				TS_TRACE(sprint("z=",z_from_scratch));
			}

			TS_ASSERT_LESS_THAN( norm_Linf(X-E), 1E-9);
		}
	}


};






#endif