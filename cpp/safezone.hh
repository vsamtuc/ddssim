#ifndef __SAFEZONE_HH__
#define __SAFEZONE_HH__


#include "mathlib.hh"
#include "agms.hh"


/**
	This file contains computational code for defining admissible
	regions, safe zone functions, distance functions, etc.
  */

namespace dds { 

using namespace agms;


/**
	A base class for safe zone functions, marking them
	as valid or not. Invalid safe zones are e.g. not-initialized
  */
struct safezone_base
{
	bool isvalid;
	safezone_base() : isvalid(true){}
	safezone_base(bool v) : isvalid(v) {}
};



/**
	Safezone for boolean quorum queries.

	A \f$(n,k)\f$-quorum boolean function is the boolean function 
	which is true if and only if \f$k\f$ or more of its
	inputs are true. In particular:
	* For \f$k=1\f$ this function is the logical OR.
	* For \f$k=n\f$ it is the logical AND.
	* For \f$k=(n+1)/2\f$, it is the majority function.

	This implementation computes an expensive version of the
	safe zone function. In particular, it preserves eikonality
	and non-redundancy of the inputs. 

	Because of its complexity, evaluating this function is expensive:
	each invocation takes time \f$O(k\binom{l}{k-1})\f$ which can
	be quite high (\f$l \leq n \f$ is the number of true components
	in the original estimate vector, or, equivalently, the number
	of positive elements of vector \f$zE\f$ passed at construction). 

	Because it is so expensive for large \f$n\f$, a fast implementation
	is also available.

	@see quorum_safezone_fast
  */
struct quorum_safezone : safezone_base
{
	size_t n;	/// the number of inputs
	size_t k;	/// the lower bound on true inputs
	Index L;  	/// the legal inputs index from n to zetaE
	Vec zetaE;	/// the reference vector's zetas, where zE >= 0.

	quorum_safezone();
	quorum_safezone(const Vec& zE, size_t _k);


	void prepare(const Vec& zE, size_t _k);

	double operator()(const Vec& zX);

};



/**
	Fast, non-eikonal safezone function for a boolean quorum query.

	This safezone function defines the same safezone as the
	one defined by the `quorum_safezone` class. Its advantage is that 
	it is quite efficient: each call takes \f$O(l)\f$ time.
	Its drawback is that it is not eikonal in general.

	@see quorum_safezone
  */
struct quorum_safezone_fast : quorum_safezone
{

	using quorum_safezone::quorum_safezone;

	double operator()(const Vec& zX);

};





/**
	The safe zone function for the upper bound on the selfjoin estimate of an AGMS sketch.

	Let \f$ X = [X_1, ... , X_d]\f$ be am AGMS sketch of depth \f$d\f$.
	The admissible region is 
	\f[ A =\{ X | \median{X_i^2 \,|\, i=1,\ldots , D}  \leq T \}.\f]

	The safe zone is defined as follows:
	Let  \f$ D \f$ be the set \f$ [0:d)\f$ where \f$d \f$ is the depth of
	the sketch and let \f$ E = [E_1, \ldots, E_d ]\f$ be the reference point.
	Define 
	\f[   \zeta_i(X_i) = \sqrt{T} - \|X_i\| \f]
	as the safe zone for each individual condition \f$ X_i^2 \leq T\f$.

	The overall safe zone is defined as the median quorum over these
	values.

  */
struct selfjoin_agms_safezone_upper_bound : safezone_base
{
	double sqrt_T;		// threshold above
	quorum_safezone Median;

	typedef Vec incremental_state;

	selfjoin_agms_safezone_upper_bound() : safezone_base(false) {}

	selfjoin_agms_safezone_upper_bound(const sketch& E, double T);

	double operator()(const sketch& X); 

	double with_inc(incremental_state& incstate, const sketch& X);

	double inc(incremental_state& incstate, const delta_vector& DX);

};



/**
	The safe zone function for the lower bound on the selfjoin estimate 
	of an AGMS sketch.

	Let \f$ X = [X_1, ... , X_d]\f$ be am AGMS sketch of depth \f$d\f$.
	The admissible region is 
	\f[ A =\{ X | \median{X_i^2 \,|\, i=1,\ldots , D}  \geq T \}.\f]

	The safe zone is defined as follows:
	Let  \f$ D \f$ be the set \f$ [0:d)\f$ where \f$d \f$ is the depth of
	the sketch and let \f$ E = [E_1, \ldots, E_d ]\f$ be the reference point.
	Define 
	\f[   \zeta_i(X_i) = X_i \frac{E_i}{\|E_i \|} - \sqrt{T}  \f]
	as the safe zone for each individual condition \f$ X_i^2 \geq T\f$.

	The overall safe zone is defined as the median quorum over these
	values.

  */
struct selfjoin_agms_safezone_lower_bound : safezone_base
{
	sketch Ehat;		// normalized reference vector
	double sqrt_T;			// threshold above

	quorum_safezone Median;  //  the median

	typedef Vec incremental_state;

	selfjoin_agms_safezone_lower_bound() : safezone_base(false) {}

	selfjoin_agms_safezone_lower_bound(const sketch& E, double T);

	double operator()(const sketch& X);

	double with_inc(incremental_state& incstate, const sketch& X);

	double inc(incremental_state& incstate, const delta_vector& DX);

};


struct selfjoin_query;


/**
	Safezone for the condition  Tlow <= dot_est(X) <= Thigh.

	This is essentially a wrapper for two safezones,
	one for upper bound and one for lower bound.

	@see selfjoin_agms_safezone_upper_bound
	@see selfjoin_agms_safezone_lower_bound
 */
struct selfjoin_agms_safezone : safezone_base
{
	selfjoin_agms_safezone_lower_bound lower_bound;	// Safezone for sk^2 >= Tlow
	selfjoin_agms_safezone_upper_bound upper_bound;	// Safezone for sk^2 <= Thigh

	struct incremental_state
	{
		selfjoin_agms_safezone_lower_bound::incremental_state lower;
		selfjoin_agms_safezone_upper_bound::incremental_state upper;		
	};

	selfjoin_agms_safezone() : safezone_base(false) {}

	selfjoin_agms_safezone(const sketch& E, double Tlow, double Thigh);

	selfjoin_agms_safezone(selfjoin_query& q);
	selfjoin_agms_safezone& operator=(selfjoin_agms_safezone&&)=default;

	// from-scratch computation
	double operator()(const sketch& X);

	double with_inc(incremental_state& incstate, const sketch& X);

	double inc(incremental_state& incstate, const delta_vector& DX);
};




/*
	TODO: Maybe this does not belong here!
 */
struct selfjoin_query
{
	double beta; 	// the overall precision
	sketch E;   	// The current estimate for the stream

	double Qest; 	// current estimate
	double Tlow;    // admissible region: Tlow <= Q(t) <= Thigh
	double Thigh;   // 

	selfjoin_agms_safezone safe_zone;
	double zeta_E;

	selfjoin_query(double _beta, projection _proj)
	: beta(_beta), E(_proj)
	{
		assert( norm_Linf(E)==0.0);
		assert(_proj.epsilon() < beta);
		compute();
		assert(fabs(zeta_E-sqrt( (_proj.depth()+1)/2))<1E-15);
	}


	void update_estimate(const sketch& newE)
	{
		// compute the admissible region
		E += newE;
		compute();
	}

	void compute()
	{
		Qest = dot_est(E);

		if(Qest>0) {
			Tlow = (1+E.proj.epsilon())*Qest/(1.0+beta);
			Thigh = (1-E.proj.epsilon())*Qest/(1.0-beta);
			safe_zone = std::move(selfjoin_agms_safezone(*this)); 
		}
		else {
			Tlow = 0.0; Thigh=1.0;
			safe_zone = selfjoin_agms_safezone(E,0.0,1.0);
		}

		zeta_E = safe_zone(E);
	}
};




} // end namespace dds


#endif