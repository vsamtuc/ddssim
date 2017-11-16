#ifndef __SAFEZONE_HH__
#define __SAFEZONE_HH__


#include "query.hh"
#include "mathlib.hh"
#include "agms.hh"


/**
	This file contains computational code for defining admissible
	regions, safe zone functions, distance functions, etc.
  */

namespace gm { 

using namespace dds;
using namespace agms;

/**
	A base class for safe zone functions, marking them
	as valid or not. Invalid safe zones are e.g. not-initialized
  */
struct safezone_func_base
{
	bool isvalid;
	safezone_func_base() : isvalid(true){}
	safezone_func_base(bool v) : isvalid(v) {}
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
struct quorum_safezone : safezone_func_base
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
struct selfjoin_agms_safezone_upper_bound : safezone_func_base
{
	double sqrt_T;		// threshold above
	quorum_safezone Median;

	typedef Vec incremental_state;

	selfjoin_agms_safezone_upper_bound() : safezone_func_base(false) {}

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
struct selfjoin_agms_safezone_lower_bound : safezone_func_base
{
	sketch Ehat;		// normalized reference vector
	double sqrt_T;			// threshold above

	quorum_safezone Median;  //  the median

	typedef Vec incremental_state;

	selfjoin_agms_safezone_lower_bound() : safezone_func_base(false) {}

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
struct selfjoin_agms_safezone : safezone_func_base
{
	selfjoin_agms_safezone_lower_bound lower_bound;	// Safezone for sk^2 >= Tlow
	selfjoin_agms_safezone_upper_bound upper_bound;	// Safezone for sk^2 <= Thigh

	struct incremental_state
	{
		selfjoin_agms_safezone_lower_bound::incremental_state lower;
		selfjoin_agms_safezone_upper_bound::incremental_state upper;		
	};

	selfjoin_agms_safezone() : safezone_func_base(false) {}

	selfjoin_agms_safezone(const sketch& E, double Tlow, double Thigh);

	selfjoin_agms_safezone(selfjoin_query& q);
	selfjoin_agms_safezone& operator=(selfjoin_agms_safezone&&)=default;


	// from-scratch computation, storing upper and lower bounds into
	// zeta_l and zeta_u. It returns min(zeta_l, zeta_u)
	double operator()(const sketch& X);
	double operator()(const sketch& X, double& zeta_l, double& zeta_u);

	// from-scratch computation with inc state, storing upper and lower bounds into
	// zeta_l and zeta_u. It returns min(zeta_l, zeta_u)
	double with_inc(incremental_state& incstate, const sketch& X);
	double with_inc(incremental_state& incstate, const sketch& X, double& zeta_l, double& zeta_u);

	// incremental computation, storing upper and lower bounds into
	// zeta_l and zeta_u. It returns min(zeta_l, zeta_u)
	double inc(incremental_state& incstate, const delta_vector& DX);
	double inc(incremental_state& incstate, const delta_vector& DX, double& zeta_l, double& zeta_u);
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

	The accuracy \f$ \epsilon >0 \f$ is relative: if \f$x_r\f$ is returned, then
	the actuall value will lie in
	an interval of size \f$ x_r\cdot (1\pm \epsilon/2)\f$.

	Accuracy is set to \f$\epsilon = 10^{-13}\f$ by default. This is close to the accuracy
	of IEEE 754 `double`.

	This method converges in about 40 iterations on average.
  */
double hyperbola_nearest_neighbor(double p, double q, double T, double epsilon=1.E-13 );


/**
	A safe zone for the problem \f$x^2 - y^2 \geq T\f$ in 2 dimensions.
	
	The reference point for this safezone is given as \f$(\xi, \psi)\f$, and
	must satisfy the condition \f$\xi^2 - \psi^2 \geq T\f$. If this is not the case,
	an \c std::invalid_argument is thrown.

	When \f$T=0\f$, there is ambiguity if \f$ \xi = 0\f$: there are two candidate safe
	zones, one is \f$ Z_{+} =  \{ x\geq |y| \} \f$ and the other is \f$ Z_{-} = \{ x \leq -|y| \} \f$. In
	this case, the function will select zone \f$ Z_{+} \f$.

  */
struct bilinear_2d_safe_zone : safezone_func_base
{
	double T;			///< threshold
	int xihat;			///< cached for case T>0
	double u, v;		//< cached for case T<=0

	/**
		\brief Default construct an invalid safe zone
	  */
	bilinear_2d_safe_zone();

	/**
		\brief Construct a valid safe zone.

		@param xi the x-coordinate of reference point \f$(\xi,\psi)\f$.
		@param psi the y-coordinate of reference point \f$(\xi,\psi)\f$.
		@param _T  the safe zone threshold

		@throws std::invalid_argument if \f$ \xi^2 - \psi^2 < T\f$.
	  */
	bilinear_2d_safe_zone(double xi, double psi, double _T);

	/**
		\brief The value of the safe zone function at \f$(x,y)\f$.
	  */
	double operator()(double x, double y) const;
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

	bilinear_2d_safe_zone sqdiff;

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

		sqdiff = bilinear_2d_safe_zone(norm_xi, norm_psi, 4.*T);

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



struct twoway_join_agms_safezone : safezone_func_base
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





template <qtype QType>
struct continuous_query;




/**
	This class wraps safezone functions. It serves as part of the protocol.

	Semantics: if `valid()` returns false, the system is in "promiscuous mode"
	i.e., every update is forwarded to the coordinator immediately.

	Else, there are two options: if a valid safezone function is given, then
	it is used. Else, the naive function is used:
	\f[  \zeta(X) = \zeta_{E} - \| U \|  \f]
	where \f$ u \f$ is the current drift vector.
  */
struct safezone
{
	selfjoin_agms_safezone* szone; // the safezone function, if any
	selfjoin_agms_safezone::incremental_state incstate; // cached here for convenience
	double incstate_naive;

	sketch* Eglobal;		// implementation detail, also used to compute the byte size
	size_t updates;			// number of updates, determines the size of the global sketch
	double zeta_E;			// This is acquired by calling the safezone 

	// invalid
	safezone() : szone(nullptr), Eglobal(nullptr), updates(0), zeta_E(-1) {};

	// valid safezone
	safezone(selfjoin_agms_safezone* sz, sketch* E, size_t upd, double zE)
	: szone(sz), Eglobal(E), updates(upd), zeta_E(zE)
	{
		assert(sz && sz->isvalid);
		assert(zE>0);
	}

	// must be valid, i.e. zE>0
	safezone(double zE) 
	:  szone(nullptr), Eglobal(nullptr), updates(0), zeta_E(zE)
	{
		assert(zE>0);
	}

	// We take these to be non-movable!
	safezone(safezone&&)=delete;
	safezone& operator=(safezone&&)=delete;

	// Copyable
	safezone& operator=(const safezone&) = default;

	inline bool naive() const { return szone==nullptr && zeta_E>0; }
	inline bool full() const { return szone!=nullptr; }
	inline bool valid() const { return full() || naive(); }

	double prepare_inc(const isketch& U, double& zeta_l, double& zeta_u)
	{
		if(full()) {
			sketch X = (*Eglobal)+U;
			return szone->with_inc(incstate, X, zeta_l, zeta_u);
		} else if(naive()) {
			return zeta_l = zeta_u = zeta_E - norm_L2_with_inc(incstate_naive, U);
		} else {
			assert(!valid());
			return NAN;
		}
	}

	double operator()(const isketch& U, double& zeta_l, double& zeta_u)
	{
		if(full()) {
			delta_vector DX = U.delta;
			DX += *Eglobal;
			return szone->inc(incstate, DX, zeta_l, zeta_u);
		}
		else if(naive()) {
			return zeta_l = zeta_u = zeta_E - norm_L2_inc(incstate_naive, U.delta);
		} else {
			assert(! valid());
			return NAN;			
		}
	}

	size_t byte_size() const {
		if(!valid()) 
			return 1;
		else if(szone==nullptr)
			return sizeof(zeta_E);
		else
			return compressed_sketch{*Eglobal, updates}.byte_size();
	}
};


template <>
struct continuous_query<qtype::SELFJOIN>
{
	typedef selfjoin_query query_type;
	typedef sketch state_vector_type;
	typedef isketch drift_vector_type;
	typedef safezone safezone_type;
};



template <>
struct continuous_query<qtype::JOIN>
{
	typedef selfjoin_query query_type;
	typedef sketch state_vector_type;
	typedef isketch drift_vector_type;
	typedef safezone safezone_type;
};




} // end namespace dds


#endif
