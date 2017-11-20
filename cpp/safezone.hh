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
struct safezone_func
{
	bool isvalid;
	safezone_func() : isvalid(true){}
	safezone_func(bool v) : isvalid(v) {}
};



/**
	Base class for a query state object.

	A query state holds the current global estimate \f$ E \f$,
	provides the query function as a virtual method,
	and maintains the current accuracy levels.

	Also, it is a factory for safe zone objects.
 */
struct query_state
{
	double Qest; 	// current estimate
	double Tlow;    // admissible region: Tlow <= Q(t) <= Thigh
	double Thigh;   // admissible region: Tlow <= Q(t) <= Thigh

	Vec E;   		// The current global estimate
	double zeta_E;	// The current value \( \zeta(E) \)

	/**
		Constructor.

		@param _D  the dimension of the query state
	  */
	query_state(size_t _D);

	/**
		\brief The dimension of the global state.

		Note: this must be equal to the value reported by 
		continuous_query
	  */
	inline size_t state_vec_size() const { return E.size(); }

	/// The query function
	virtual double query_func(const Vec& x)=0;

	/// A functional interface
	inline double operator()(const Vec& x) { return query_func(x); }
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
	is also available. Its advantage is that it is quite efficient: 
	each call takes \f$O(l)\f$ time.
	Its drawback is that it is not eikonal in general.


	@see quorum_safezone_fast
  */
struct quorum_safezone : safezone_func
{
	size_t n;	/// the number of inputs
	size_t k;	/// the lower bound on true inputs
	Index L;  	/// the legal inputs index from n to zetaE
	Vec zetaE;	/// the reference vector's zetas, where zE >= 0.
	bool eikonal = true; /// The eikonality flag

	quorum_safezone();
	quorum_safezone(const Vec& zE, size_t _k, bool _eik);


	void prepare(const Vec& zE, size_t _k);
	void set_eikonal(bool _eik) { eikonal = _eik; }


	double func_eikonal(const Vec& zX);
	double func_non_eikonal(const Vec& zX);

	inline double operator()(const Vec& zX) {
		return (eikonal) ? func_eikonal(zX) : func_non_eikonal(zX);
	}

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
struct selfjoin_agms_safezone_upper_bound : safezone_func
{
	double sqrt_T;		// threshold above
	projection proj;
	quorum_safezone Median;

	typedef Vec incremental_state;

	selfjoin_agms_safezone_upper_bound() : safezone_func(false) {}

	template<typename Iter>
	selfjoin_agms_safezone_upper_bound(const sketch_view<Iter>& E, double T, bool eikonal) 
	: 	sqrt_T(sqrt(T)), proj(E.proj), Median()
	{
		Vec dest = sqrt(dot_estvec(E));
		Median.prepare(sqrt_T - dest , (E.depth()+1)/2 );
		Median.set_eikonal(eikonal);
	}

	selfjoin_agms_safezone_upper_bound(const sketch& E, double T, bool eikonal) 
	: selfjoin_agms_safezone_upper_bound(E.view(), T, eikonal)
	{ }


	double operator()(const Vec& X); 

	double with_inc(incremental_state& incstate, const Vec& X);

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

	Note: if \f$ T\leq 0\f$, the function returns \f$+\infty \f$. 

  */
struct selfjoin_agms_safezone_lower_bound : safezone_func
{
	sketch Ehat;		// normalized reference vector
	double sqrt_T;			// threshold above

	quorum_safezone Median;  //  the median

	typedef Vec incremental_state;

	selfjoin_agms_safezone_lower_bound() : safezone_func(false) {}

	template<typename Iter>
	selfjoin_agms_safezone_lower_bound(const sketch_view<Iter>& E, double T, bool eikonal)
		: 	Ehat(E), sqrt_T((T>0.0) ? sqrt(T) : 0.0)
	{ 
		//  If T <= 0.0, the function returns +inf
		if(sqrt_T>0.0) {

			Vec dest = sqrt(dot_estvec(E));
			Median.prepare( dest - sqrt_T, (E.depth()+1)/2);
			Median.set_eikonal(eikonal);

			//
			// Normalize E: divide each row  E_i by ||E_i||
			//
			size_t L = E.width();
			for(size_t d=0; d<E.depth(); d++) {
				if(dest[d]>0.0)  {
					// Note: this is an aliased assignment, but should
					// be ok, because it is pointwise aliased!
					Vec tmp = Ehat[slice(d*L,L,1)];
					Ehat[slice(d*L,L,1)] = tmp/dest[d];
				}
				// else, if dest[d]==0, then Ehat[slice(d)] == 0! leave it
			}
		}

	}

	selfjoin_agms_safezone_lower_bound(const sketch& E, double T, bool eikonal)
	: selfjoin_agms_safezone_lower_bound(E.view(), T, eikonal)
	{ }


	double operator()(const Vec& X);

	double with_inc(incremental_state& incstate, const Vec& X);

	double inc(incremental_state& incstate, const delta_vector& DX);

};



/**
	Safezone for the condition  Tlow <= dot_est(X) <= Thigh.

	This is essentially a wrapper for two safezones,
	one for upper bound and one for lower bound.

	@see selfjoin_agms_safezone_upper_bound
	@see selfjoin_agms_safezone_lower_bound
 */
struct selfjoin_agms_safezone : safezone_func
{
	selfjoin_agms_safezone_lower_bound lower_bound;	// Safezone for sk^2 >= Tlow
	selfjoin_agms_safezone_upper_bound upper_bound;	// Safezone for sk^2 <= Thigh

	struct incremental_state
	{
		selfjoin_agms_safezone_lower_bound::incremental_state lower;
		selfjoin_agms_safezone_upper_bound::incremental_state upper;		
	};

	selfjoin_agms_safezone() : safezone_func(false) {}

	template <typename Iter>
	selfjoin_agms_safezone(const sketch_view<Iter>& E, double Tlow, double Thigh, bool eikonal)
	: 	lower_bound(E, Tlow, eikonal),
		upper_bound(E, Thigh, eikonal)
	{
		assert(Tlow < Thigh);
	}

	selfjoin_agms_safezone(const sketch& E, double Tlow, double Thigh, bool eikonal)
	: selfjoin_agms_safezone(E.view(), Tlow, Thigh, eikonal)
	{ }


	selfjoin_agms_safezone& operator=(selfjoin_agms_safezone&&)=default;


	// from-scratch computation, storing upper and lower bounds into
	// zeta_l and zeta_u. It returns min(zeta_l, zeta_u)
	double operator()(const Vec& X);
	double operator()(const Vec& X, double& zeta_l, double& zeta_u);

	// from-scratch computation with inc state, storing upper and lower bounds into
	// zeta_l and zeta_u. It returns min(zeta_l, zeta_u)
	double with_inc(incremental_state& incstate, const Vec& X);
	double with_inc(incremental_state& incstate, const Vec& X, double& zeta_l, double& zeta_u);

	// incremental computation, storing upper and lower bounds into
	// zeta_l and zeta_u. It returns min(zeta_l, zeta_u)
	double inc(incremental_state& incstate, const delta_vector& DX);
	double inc(incremental_state& incstate, const delta_vector& DX, double& zeta_l, double& zeta_u);
};




/*
	TODO: Maybe this does not belong here!
 */
struct selfjoin_query_state : query_state
{
	double beta; 	// the overall precision
	projection proj;// the sketch projection
	double epsilon; // the **assumed** precision of the sketch

	selfjoin_agms_safezone safe_zone;
	selfjoin_query_state(double _beta, projection _proj);

	void update_estimate(const Vec& newE);
	virtual double query_func(const Vec& x) override;

private:
	inline auto Eview() const { return make_sketch_view(proj, E); }
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
struct bilinear_2d_safe_zone : safezone_func
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
	  */
	bilinear_2d_safe_zone(double xi, double psi, double _T);

	/**
		\brief The value of the safe zone function at \f$(x,y)\f$.
	  */
	double operator()(double x, double y) const;
};



/**
	An eikonal safe zone function for the inner product of two vectors.

	The function computes the safezone for a constraint of the form
	\f[   X_1 X_2 \geq T  \f]
	or
	\f[   X_1 X_2 \leq T  \f]

	The incremental state is of size \f$O(n)\f$, where \f$n\f$ is the dimension
	of the vectors. The complexity of an incremental computation is \f$O(d)\f$ where
	\f$d \f$ is the size of the delta vector. 

	Each evaluation calls function \c hyperbola_nearest_neighbor at most once.
  */
struct inner_product_safe_zone
{
	bool geq;
	double T;
	Vec xihat;

	bilinear_2d_safe_zone sqdiff;

	struct incremental_state {
		double x2;
		double y2;
		Vec x,y;
	};

	/**
		Initialize a safe zone for reference point \f$(E_1, E_2)\f$, and
		for condition 
		\f[	X_1 X_2 \geq T \f]
		if \c _geq is \c true,
		or
		\f[	X_1 X_2 \leq T \f]
		if \c _geq is \c false.

		@param E the reference point
		@param _geq a boolean, designating an upper or lower bound
		@param T the threshold
	  */
	inner_product_safe_zone(const Vec& E, bool _geq, double _T);

	/**
		\brief From-scratch computation of the function.

		The complexity is \f$ O(n) \f$.
	  */
	double operator()(const Vec& X) const;

	/**
		\brief From-scratch computation of the function, with initialization of incremental state.

		The complexity is \f$ O(n) \f$. The size of the incremental state is also \f$ O(n) \f$.
	  */
	double with_inc(incremental_state& inc, const Vec& X) const;


	/**
		\brief Incremental computation of the function.

		The complexity is \f$ O(d) \f$, where \f$d\f$ is the size of \c dX.
	  */
	double inc(incremental_state& inc, const delta_vector& dX) const;


};



/**
	A safe zone function for bounding the join estimate of two AGMS sketches.

	Let \f$ X = [X_1, ... , X_d]\f$ be am AGMS sketch of depth \f$d\f$.

	Given sketches \f$X \f$ and \f$Y\f$, the join estimate is 
	\f[ Q(X,Y) =  \median{X_iY_i \,|\, i=1,\ldots , D}. \f]
	Given thresholds \f$ T_\text{low} \f$ and \f$ T_\text{high} \f$, 
	the admissible region is 
    \f[ A =\{ (X,Y) | T_\text{low} \leq Q(X,Y) \leq T_\text{hi} \}. \f]

    The safe zone is defined per the algorithms of [Garofalakis and Samoladas, 
    ICDT 2017].
  */
struct twoway_join_agms_safezone : safezone_func
{
	twoway_join_agms_safezone();

	double Tlow, Thigh;
	projection proj;
	Vec xihat, psihat;

	vector<slice> col;
	vector<bilinear_2d_safe_zone> zeta2low, zeta2high;

	quorum_safezone Median_low, Median_high;

	twoway_join_agms_safezone(const Vec& E, const projection& _proj, 
								double _Tlow, double _Thigh, bool eikonal)
		: Tlow(_Tlow), Thigh(_Thigh), proj(_proj),
		  xihat(_proj.size()), psihat(_proj.size())
	{
		assert(E.size() == 2*proj.size());
		assert(Tlow < Thigh);

		// initialize the arrays
		xihat = E[slice(0, proj.size(),1)] + E[slice(proj.size(), proj.size(),1)];
		psihat = E[slice(0, proj.size(),1)] - E[slice(proj.size(), proj.size(),1)];

		Vec norm_xi = sqrt(dot_estvec(make_sketch_view(proj, xihat)));
		Vec norm_psi = sqrt(dot_estvec(make_sketch_view(proj, psihat)));

		// create the slices and the 
		col.reserve(proj.depth());
		zeta2low.reserve(proj.depth());
		zeta2high.reserve(proj.depth());

		Vec zeta_Elow(proj.depth());
		Vec zeta_Ehigh(proj.depth());

		Vec tmp(proj.width());

		for(size_t i=0; i<proj.depth(); i++) {
			col.emplace_back(i*proj.width(), proj.width(), 1);
			zeta2low.emplace_back(norm_xi[i], norm_psi[i], 4.*Tlow);
			zeta_Elow[i] = zeta2low[i](norm_xi[i], norm_psi[i])*sqrt(0.5);

			zeta2high.emplace_back(norm_psi[i], norm_xi[i], -4.*Thigh);
			zeta_Ehigh[i] = zeta2high[i](norm_psi[i], norm_xi[i])*sqrt(0.5);

			if(norm_xi[i]>0.0) {
				tmp = norm_xi[i];
				xihat[col[i]] /= tmp;
			} 
			else {
				tmp = 0.0;
				xihat[col[i]] = tmp;
			}

			if(norm_psi[i]>0.0) {
				tmp = norm_psi[i];
				psihat[col[i]] /= tmp;
			} 
			else {
				tmp = 0.0;
				psihat[col[i]] = tmp;
			}

		}

		Median_low.prepare(zeta_Elow, (proj.depth()+1)/2);
		Median_low.set_eikonal(eikonal);

		Median_high.prepare(zeta_Ehigh, (proj.depth()+1)/2);
		Median_high.set_eikonal(eikonal);
	}

	twoway_join_agms_safezone& operator=(twoway_join_agms_safezone&&)=default;

	inline Vec dot_col(const Vec& v1, const Vec& v2) const {
		return dot_estvec(make_sketch_view(proj, v1), make_sketch_view(proj,v2));
	}
	inline Vec dot_col(const Vec& v) const {
		return dot_estvec(make_sketch_view(proj, v));
	}

	double zeta_bound(const Vec& u1, const Vec& u1hat, const Vec& u2, 
		const vector<bilinear_2d_safe_zone>& zeta2bound,
		quorum_safezone& Median)
	{
        Vec x2 = dot_col(u1, u1hat);
        Vec y2 = sqrt(dot_col(u2));

        Vec zetaX(proj.depth());
        for(size_t i=0; i<proj.depth(); i++)
        	zetaX[i] = zeta2bound[i](x2[i], y2[i])*sqrt(0.5);

        return Median(zetaX);		
	}

	// from-scratch computation
	double operator()(const Vec& X)
	{
		assert(X.size() == 2*proj.size());

        slice s1(0,xihat.size(),1);
        slice s2(xihat.size(),xihat.size(),1);

        Vec x = X[s1]+X[s2];
        Vec y = X[s1]-X[s2];

        // Compute low
        double zeta_low = zeta_bound(x, xihat, y, zeta2low, Median_low);

        // Compute high
        double zeta_high = zeta_bound(y, psihat, x, zeta2high, Median_high);

        return min(zeta_low, zeta_high);
	}


	struct incremental_state
	{ };


	double with_inc(incremental_state& incstate, const Vec& X);

	double inc(incremental_state& incstate, const delta_vector& DX);
};


struct twoway_join_query : query_state
{
	double beta;
	double epsilon;
	sketch E1, E2;


	twoway_join_agms_safezone safe_zone;

	twoway_join_query(double _beta, projection _proj);
	void update_estimate1(const sketch&);
	void update_estimate2(const sketch&);

	virtual double query_func(const Vec& x) override;

};






} // end namespace gm


#endif
