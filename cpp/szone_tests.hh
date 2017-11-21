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
	void test_quorum_est()
	{

		Vec zE = { 13.0, 17, 26, 11, -33, 31, 52 };

		// Test that the eikonal and non-eikonal safe zones are equal

		quorum_safezone szne(zE, (zE.size()+1)/2, false);
		TS_ASSERT_EQUALS(szne.n, 7);
		TS_ASSERT_EQUALS(szne.k, 4);
		TS_ASSERT_EQUALS(szne.L.size(), 6);

		quorum_safezone sze(zE, (zE.size()+1)/2, true);
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
			quorum_safezone sz(E, N, true);
			quorum_safezone szf(E, N, true);

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

			quorum_safezone sz(E, 1, true);
			quorum_safezone szf(E,1, false);

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

			selfjoin_agms_safezone_upper_bound sz(E, 1.1*Emed, true);

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

			selfjoin_agms_safezone_lower_bound sz(E, 0.9*Emed, true);

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

			selfjoin_agms_safezone sz(E, 0.9*Emed, 1.1*Emed, true);
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

		selfjoin_agms_safezone sz(E, 0.8*Emed, 1.2*Emed, true);

	
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



	void test_hyperbola_nn_0()
	{
		using gm::hyperbola_nearest_neighbor; 
		TS_ASSERT_EQUALS(hyperbola_nearest_neighbor(0,0,0), 0);

		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(1,-3,0), 0, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(1,-1,0), 0, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(1,-0.9,0), 0.05, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(1,0,0), 0.5, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(1,1,0), 1, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(1,2,0), 1.5, 1E-9);

		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(-1,-3,0), 0, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(-1,-1,0), 0, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(-1,-0.9,0), -0.05, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(-1,0,0), -0.5, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(-1,1,0), -1, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(-1,2,0), -1.5, 1E-9);


		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(0,1,0), 0.5, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(0,10,0), 5.0, 1E-9);
		TS_ASSERT_DELTA(hyperbola_nearest_neighbor(0,-1,0), 0.0, 1E-9);
	}


	void test_hyperbola_nn_scale()
	{

		for(double xi=-10; xi<=10; xi+=0.11) {
			double psi = sqrt(xi*xi+1.0);
			for(double t=0.49; t>=-100.0; t+=((t>=-1.0)?-0.01:t) ) {
				double p = xi-2.0*t*xi;
				double q = psi + 2.0*t*psi;

				double xi_ret = hyperbola_nearest_neighbor(p,q,1.0);

				TS_ASSERT_DELTA(xi_ret, xi, 1E-9);

				//binc::print("p=",p," q=",q," xi=",xi,"xi_ret=",xi_ret);

				for(double T = 1E-5; T<= 1E5; T*=10.) {
					double sqrtT = sqrt(T);
					double xi_s = hyperbola_nearest_neighbor(p*sqrtT, q*sqrtT, T)/sqrtT;
					TS_ASSERT_DELTA(xi_s, xi_ret, 1E-9 );

				}
			}
		}
	}


	void test_bilinear_2d_T_zero()
	{
		bilinear_2d_safe_zone zeta( 1., 0., 0. );

		TS_ASSERT_DELTA(zeta(1.,0.), 1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(1.,1.), 0, 1.E-12);
		TS_ASSERT_DELTA(zeta(0.,1.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(-1.,0.), -1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

		zeta = {0.,0.,0.}; // this should yield the same safe zone as above
		TS_ASSERT_DELTA(zeta(1.,0.), 1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(1.,1.), 0, 1.E-12);
		TS_ASSERT_DELTA(zeta(0.,1.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(-1.,0.), -1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

		zeta = {1.,1.,0.}; // this should yield the same safe zone as above
		TS_ASSERT_DELTA(zeta(1.,0.), 1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(1.,1.), 0, 1.E-12);
		TS_ASSERT_DELTA(zeta(0.,1.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(-1.,0.), -1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

		zeta = {-1.,1.,0.}; // this should define the left-facing cone
		TS_ASSERT_DELTA(zeta(1.,0.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(1.,1.), -sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(0.,1.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(-1.,0.), 1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

		zeta = {-1.,0.5,0.}; // this should define the left-facing cone
		TS_ASSERT_DELTA(zeta(1.,0.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(1.,1.), -sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(0.,1.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(-1.,0.), 1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

		zeta = {-1.,-0.5,0.}; // this should define the left-facing cone
		TS_ASSERT_DELTA(zeta(1.,0.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(1.,1.), -sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(0.,1.), -1./sqrt(2), 1.E-12);
		TS_ASSERT_DELTA(zeta(-1.,0.), 1/sqrt(2), 1.E-12); //note: the SDF here would yield -1 < -1/\sqrt{2}!

		// When the ref.point is not in the zone, throw!
		//TS_ASSERT_THROWS( (zeta={0,1,0}), std::invalid_argument  );
	}

	void test_bilinear_2d_T_pos()
	{
		bilinear_2d_safe_zone zeta { 1, 0, 1};

		TS_ASSERT_EQUALS(zeta.u, 0.0);
		TS_ASSERT_EQUALS(zeta.v, 0.0);
		TS_ASSERT_EQUALS(zeta.T, 1.0);
		TS_ASSERT_EQUALS(zeta.xihat, 1);

		TS_ASSERT_DELTA(zeta(sqrt(2),1), 0., 1E-12);
		TS_ASSERT_DELTA(zeta(sqrt(5),2), 0., 1E-12);
		TS_ASSERT_DELTA(zeta(sqrt(5),2), 0., 1E-12);

		for(double a=0.1; a<10; a+=0.1) {
			double d = sqrt(2*sq(a)+1);
			TS_ASSERT_DELTA(zeta(0,2*a), -d, 1E-12);
			TS_ASSERT_DELTA(zeta(2.0*sqrt(sq(a)+1.0),0), d, 1E-12);
		}

		zeta = {-1.5, -0.5, 1};

		TS_ASSERT_DELTA(zeta(-sqrt(2),1), 0., 1E-12);
		TS_ASSERT_DELTA(zeta(-sqrt(5),2), 0., 1E-12);
		TS_ASSERT_DELTA(zeta(-sqrt(5),2), 0., 1E-12);

		for(double a=0.1; a<10; a+=0.1) {
			double d = sqrt(2*sq(a)+1);
			TS_ASSERT_DELTA(zeta(0,2*a), -d, 1E-12);
			TS_ASSERT_DELTA(zeta(-2.0*sqrt(sq(a)+1.0),0), d, 1E-12);
		}

		// When the ref.point is not in the zone, throw!
		//TS_ASSERT_THROWS( (zeta={-1,0.5,1}), std::invalid_argument  );		
	}


	void test_bilinear_2d_T_neg()
	{
		bilinear_2d_safe_zone zeta { 1, 0, -1};

		double uu = 0.5;
		double vv = sqrt(1.+sq(uu));
		double norm_uuvv = sqrt(sq(uu)+sq(vv));
		TS_ASSERT_DELTA(zeta.u, uu/norm_uuvv, 1E-12);
		TS_ASSERT_DELTA(zeta.v, vv/norm_uuvv, 1E-12);

		TS_ASSERT_DELTA(zeta(0,sqrt(5)), -sqrt(1.5), 1E-12);
		TS_ASSERT_DELTA(zeta(0,-sqrt(5)), -sqrt(1.5), 1E-12);
		TS_ASSERT_DELTA(zeta(0,0), 1/norm_uuvv, 1E-12);

		zeta = {0, -0.5, -1};
		TS_ASSERT_DELTA(zeta.u, 0.0, 1E-16);
		TS_ASSERT_DELTA(zeta.v, 1.0, 1E-16);
		TS_ASSERT_DELTA(zeta(0,0), 1, 1E-16);
		TS_ASSERT_DELTA(zeta(10,0), 1, 1E-16);
		TS_ASSERT_DELTA(zeta(-100,0), 1, 1E-16);

		TS_ASSERT_DELTA(zeta(1E6,1), 0, 1E-16);
		TS_ASSERT_DELTA(zeta(-1E6,-2), -1, 1E-16);

	}



	void inner_product_check(bilinear_2d_safe_zone& sz2, inner_product_safe_zone& sz, const Vec& X)
	{
		TS_ASSERT_DELTA( 
			sz2(sqrt(0.5)*(X[0]+X[1]),sqrt(0.5)*(X[0]-X[1])),
			sz(X),
			1E-12
		  );		
	}


	void test_inner_product()
	{

		// Check a bunch of safe zones
		for(double T=-10.0; T<= 10.0; T+=2.)
		for(double E1=-5.; E1<=5.0; E1+=0.5)
		for(double E2=-5.; E2<=5.0; E2+=0.5) 
		{
			Vec E {E1,E2};

			{
				auto sz2 = bilinear_2d_safe_zone(sqrt(0.5)*(E[0]+E[1]),sqrt(0.5)*(E[0]-E[1]), 2.*T);
				auto sz = inner_product_safe_zone(E, true, T);

				if(E1*E2 >= T) 
				{
					for(size_t i=0;i<150;i++) {
						Vec X = uniform_random_vector(2, -10.0, 10.0);
						TS_ASSERT_DELTA( 
							sz2(sqrt(0.5)*(X[0]+X[1]),sqrt(0.5)*(X[0]-X[1])),
							sz(X),
							1E-12
						  );		
					}
				} else {
					TS_ASSERT( sz2(sqrt(0.5)*(E[0]+E[1]),sqrt(0.5)*(E[0]-E[1])) < 0.0 );
					TS_ASSERT( sz(E) < 0.0 );
				}
			}


			{
				auto sz2 = bilinear_2d_safe_zone(sqrt(0.5)*(E[0]-E[1]),sqrt(0.5)*(E[0]+E[1]), -2.*T);
				auto sz = inner_product_safe_zone(E, false, T);

				if(E1*E2 <= T) 
				{
					for(size_t i=0;i<150;i++) {
						Vec X = uniform_random_vector(2, -10.0, 10.0);
						TS_ASSERT_DELTA( 
							sz2(sqrt(0.5)*(X[0]-X[1]),sqrt(0.5)*(X[0]+X[1])),
							sz(X),
							1E-12
						  );		
					}
				} else {
					TS_ASSERT( sz2(sqrt(0.5)*(E[0]-E[1]),sqrt(0.5)*(E[0]+E[1])) < 0.0 );
					TS_ASSERT( sz(E) < 0.0 );
				}
			} 
		}
	}

	void test_inner_product_inc1()
	{
		TS_TRACE("inner_product_inc1");
		projection proj(3, 4);
		sketch E(proj);
		E = uniform_random_vector(proj.size(), -10, 10);

		size_t n = proj.size()/2;
		Vec E1 = E[slice(0,n,1)];
		Vec E2 = E[slice(n,n,1)];
		double T = (E1*E2).sum() - 1 ;

		inner_product_safe_zone zeta(E, true, T);
		TS_ASSERT_LESS_THAN(0 , zeta(E));

		inner_product_safe_zone::incremental_state inc;
		double zeta_from_scratch = zeta(E);
		double zeta_with_inc = zeta.with_inc(inc, E);
		TS_ASSERT_DELTA( zeta_from_scratch, zeta_with_inc, 1E-16 );
		TS_ASSERT_EQUALS( inc.x.size(), proj.size()/2);
		TS_ASSERT_EQUALS( inc.y.size(), proj.size()/2);		

		delta_vector dE(proj.depth());
		buffered_dataset dset = make_uniform_dataset(1,1,100000,100);
		for(auto&& rec : dset) {
			E.update(dE, rec.key, rec.upd);
			TS_ASSERT_DELTA(zeta(E), zeta.inc(inc, dE), 1E-12);
		}

		// update just one element
		delta_vector dE1(1);
		for(size_t p=0; p<E.size(); p++){
			dE1.index[0] = p; 
			dE1.xold = E[dE1.index];
			E[p] += 1.0;
			dE1.xnew = E[dE1.index];

			TS_ASSERT_DELTA(zeta(E), zeta.inc(inc, dE1), 1E-12);					
		}
	}

	void test_inner_product_inc2()
	{
		TS_TRACE("inner_product_inc2");
		projection proj(3, 4);
		sketch E(proj);
		E = uniform_random_vector(proj.size(), -10, 10);

		size_t n = proj.size()/2;
		Vec E1 = E[slice(0,n,1)];
		Vec E2 = E[slice(n,n,1)];
		double T = (E1*E2).sum() + 1 ;

		inner_product_safe_zone zeta(E, false, T);
		TS_ASSERT_LESS_THAN(0 , zeta(E));

		inner_product_safe_zone::incremental_state inc;
		double zeta_from_scratch = zeta(E);
		double zeta_with_inc = zeta.with_inc(inc, E);
		TS_ASSERT_DELTA( zeta_from_scratch, zeta_with_inc, 1E-16 );
		TS_ASSERT_EQUALS( inc.x.size(), proj.size()/2);
		TS_ASSERT_EQUALS( inc.y.size(), proj.size()/2);		

		delta_vector dE(proj.depth());
		buffered_dataset dset = make_uniform_dataset(1,1,100000,100);
		for(auto&& rec : dset) {
			E.update(dE, rec.key, rec.upd);
			TS_ASSERT_DELTA(zeta(E), zeta.inc(inc, dE), 1E-12);
		}

		// update just one element
		delta_vector dE1(1);
		for(size_t p=0; p<E.size(); p++){
			dE1.index[0] = p; 
			dE1.xold = E[dE1.index];
			E[p] += 1.0;
			dE1.xnew = E[dE1.index];

			TS_ASSERT_DELTA(zeta(E), zeta.inc(inc, dE1), 1E-12);					
		}
	}


	void test_twoway_join_agms_safezone1()
	{
		projection proj(3,4);

		size_t D = proj.size();
		Vec E = uniform_random_vector(2*D, -10, 10);
		Vec E1 = E[slice(0,D,1)];
		Vec E2 = E[slice(D,D,1)];

		double E1E2 = dot_est(proj(E1), proj(E2));
		double Tlow = E1E2-0.1*fabs(E1E2);
		double Thigh = E1E2+0.1*fabs(E1E2);

		twoway_join_agms_safezone zeta(E, proj, Tlow, Thigh, true);

		double zeta_E = zeta(E);
		TS_ASSERT_LESS_THAN_EQUALS(0.0, zeta_E);
		TS_ASSERT_LESS_THAN_EQUALS(zeta(2.*E), 0.0);


		// Check safe zone conformity
		size_t count_safe = 0, count_admissible=0;
		size_t N = 1000;
		double rho = fabs(zeta_E)/sqrt(2.0*D);
		for(size_t i=0; i<N; i++) {
			Vec X = E + uniform_random_vector(2*D, -10.0*rho, 10.0*rho);
			Vec X1 = X[slice(0,D,1)];
			Vec X2 = X[slice(D,D,1)];
			double X1X2 = dot_est(proj(X1), proj(X2));
			bool admissible = (Tlow <= X1X2) && (X1X2 <= Thigh);
			bool safe = (zeta(X) >= 0);
			if(admissible) count_admissible++;
			if(safe) count_safe++;
			TS_ASSERT( safe <= admissible );
		}
	}


	void test_twoway_join_agms_safezone2()
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

		buffered_dataset dset = make_uniform_dataset(2,1,100000,10000);

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

		for(auto&& rec : dset) {
			TS_ASSERT(rec.sid==1 || rec.sid==2);
			X[rec.sid-1].update(dX, rec.key, rec.upd);
			if(rec.sid==2) dX.index += D;

			double z_from_scratch = zeta(S);
			double z_inc = zeta.inc(inc, dX);
			TS_ASSERT_DELTA( z_from_scratch , z_inc , 1E-10 );
		}
	}

};






#endif