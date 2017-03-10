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
	double epsilon; // the assumed precision of the sketch
	sketch E;   	// The current estimate for the stream

	double Qest; 	// current estimate
	double Tlow;    // admissible region: Tlow <= Q(t) <= Thigh
	double Thigh;   // 

	selfjoin_agms_safezone safe_zone;
	double zeta_E;

	selfjoin_query(double _beta, projection _proj);

	void update_estimate(const sketch& newE);

private:
	void compute();
};


/*
	Two-way join queries (inner product)
 */


/**
	Return the nearest point to the hyperbola.

	Given point \f$ (p,q) \f$, and \f$T \geq 0 \f$, 
	this method returns a value \f$\xi\f$, such that the distance
	to the curve \f$y(x) = \sqrt{x^2+T}\f$ is minimum.

	The method used to find the root is classic bisection (aka binary search),
	for finding the root of function
	\f[  g(x) = 2 - p/x - q/y(x)  \f]

	The accuracy \f$ epsilon >0 \f$ is relative: if \f$x_r\f$ is returned, then
	the actuall value will lie in
	an interval of size \f$ x_r (1\pm \epsilon/2)\f$.

	Accuracy is set to \f$\epsilon = 10^{-13}\f$ by default. This is close to the accuracy
	of IEEE 754 `double`.

	This method converges in about 40 iterations on average.
  */
double hyberbola_nearest_neighbor(double p, double q, double T, double epsilon=1.E-13 );


/**
	A safe zone for the problem \f$x^2 - y^2 \geq T\f$ in 2 dimensions.
	
	The reference point for this safezone is given as \f$(\xi, \psi)\f$, and
	must satisfy the condition \f$\xi^2 - \psi^2 \geq T\f$. 

	When \f$T=0\f$, the function expects \f$ \xi \neq 0\f$. That is, a safe zone
	cannot have non-empty interior.
  */
struct square_difference_2d_safe_zone : safezone_base
{
	double T;			// threshold
	int xihat;			// cached for case T>0
	double u, v;		// cached for case T<=0

	square_difference_2d_safe_zone() {}

	square_difference_2d_safe_zone(double xi, double psi, double _T)
	: T(_T), xihat(sgn(xi))
	{
		if(sq(xi)-sq(psi) < T)
			throw std::invalid_argument("the reference point is non-admissible");
		if(T==0 and xi==0) 
			throw std::invalid_argument("the safe zone has empty interior");

		// cache the conic safe zone, if applicable
		if(T<0) {
			u = hyberbola_nearest_neighbor(xi, fabs(psi), -T);
			v = sqrt(sq(u)-T);
			// eikonalize
			double norm_u_v = sqrt(sq(u)+sq(v));
			assert(norm_u_v > 0);
			u /= norm_u_v;
			v /= norm_u_v;
			T /= norm_u_v;
		}
	}

	double operator()(double x, double y) const {
		if(T>0) {
			// compute the signed distance function of the set $\{ x >= \sqrt{y^2+T} \}$.
			double x_xihat = x*xihat;

			int sgn_delta = sgn( x_xihat - sqrt(sq(y)+T) );

			double v = hyberbola_nearest_neighbor(y, x_xihat, T);
			double u = sqrt(sq(v)+T);

			return sgn_delta*sqrt(sq(x_xihat - u) + sq(y - v));
		} 
		else {
			return u*x - v*fabs(y) - T;
		} 
	}
};



/**
	A safe zone function for the inner product of two vectors.

	The function computes the safezone for a constraint of the form
	\f[  \pm X_1 X_2 \geq T  \f]
	Thus, this kind of constraint can express both upper and lower bounds on the
	product.

  */
struct inner_product_safe_zone
{
	bool minus;
	double T;
	Vec xihat;

	square_difference_2d_safe_zone sqdiff;

	/**
		Initialize a safe zone for reference point \f$(E_1, E_2)\f$, and
		for condition
		\f[	(-1)^\text{minus} X_1 X_2 \geq T. \f]

	  */
	inner_product_safe_zone(const Vec& E1, const Vec& E2, bool _minus, double _T)
	: minus(_minus), T(_T)
	{
		assert(E1.size()==E2.size());

		Vec xi = E1+E2;
		Vec psi = E1-E2;
		if(minus) xi.swap(psi);

		double norm_xi = norm_L2(xi);
		double norm_psi = norm_L2(psi);

		sqdiff = square_difference_2d_safe_zone(norm_xi, norm_psi, 4.*T);

		if(norm_xi>0)
			xihat = xi/norm_xi;
		else
			xihat = Vec(0.0, E1.size());
	}

	double operator()(const Vec& X1, const Vec& X2) const
	{
		Vec x = X1+X2;
		Vec y = X1-X2;
		if(minus) x.swap(y);

		double x2 = dot(x, xihat);
		double y2 = norm_L2(y);

		return sqdiff(x2, y2) / sqrt(2);
	}

};




struct twoway_join_query;

struct twoway_join_agms_safezone_lower_bound
{

};

struct twoway_join_agms_safezone_upper_bound
{

};



struct twoway_join_agms_safezone : safezone_base
{

	selfjoin_agms_safezone_lower_bound lower_bound;	// Safezone for sk^2 >= Tlow
	selfjoin_agms_safezone_upper_bound upper_bound;	// Safezone for sk^2 <= Thigh

	struct incremental_state
	{
		selfjoin_agms_safezone_lower_bound::incremental_state lower;
		selfjoin_agms_safezone_upper_bound::incremental_state upper;		
	};

	twoway_join_agms_safezone();

	twoway_join_agms_safezone(const sketch& E, double Tlow, double Thigh);

	twoway_join_agms_safezone(twoway_join_query& q);
	twoway_join_agms_safezone& operator=(twoway_join_agms_safezone&&)=default;

	// from-scratch computation
	double operator()(const sketch& X);

	double with_inc(incremental_state& incstate, const sketch& X);

	double inc(incremental_state& incstate, const delta_vector& DX);


};


struct twoway_join_query
{
	double beta;
	double epsilon;
	sketch E1, E2;


	double Qest;
	double Tlow;
	double Thigh;

	twoway_join_agms_safezone safe_zone;
	double zeta_E;

	twoway_join_query(double _beta, projection _proj);
	void update_estimate1(const sketch&);
	void update_estimate2(const sketch&);

};


} // end namespace dds


#endif