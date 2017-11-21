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
	Abstract base class for a safe zone function wrapper.

	This class is specialized by subclasses to access different safe zone functions.
	It abstracts safe zone computations by being a factory incremental state objects and
	providing access to a function. Also, it provides communication cost information to
	the middleware.
 */
struct safezone_func_wrapper
{
	virtual void* alloc_incstate()=0;
	virtual void free_incstate(void*)=0;
	virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U)=0;
	virtual double compute_zeta(void* inc, const Vec& U)=0;
	virtual size_t state_size() const = 0;	
};


/**
	Template for providing a standard implementation for safezone wrapping.

	This is provided for convenience.
  */
template <typename SZFunc, typename IncState = typename SZFunc::incremental_state >
struct std_safezone_func_wrapper : safezone_func_wrapper
{
	typedef SZFunc function_type;
	typedef IncState incremental_state;

	SZFunc& func;
	size_t ssize;

	std_safezone_func_wrapper(SZFunc& _func, size_t _ssize) 
		: func(_func), ssize(_ssize) 
	{ }

	virtual void* alloc_incstate() override 
	{
		return new incremental_state;
	}
	virtual void free_incstate(void* ptr) override 
	{
		delete static_cast<incremental_state*>(ptr);
	}
	virtual size_t state_size() const override 
	{
		return ssize;
	}
};



/**
	Base class for a query state object.

	A query state holds the current global estimate \f$ E \f$,
	provides the query function as a virtual method,
	and maintains the current accuracy levels.

	Also, it is a factory for safe zone objects.
 */
struct query_state : safezone_func_wrapper
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
	virtual size_t state_size() const override { return E.size(); }

	/// The query function
	virtual double query_func(const Vec& x)=0;

	/// Resetting the state
	virtual void update_estimate(const Vec& newE)=0;

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

	void update_estimate(const Vec& newE) override;
	virtual double query_func(const Vec& x) override;

	virtual void* alloc_incstate() override;
	virtual void free_incstate(void*) override;
	virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U) override;
	virtual double compute_zeta(void* inc, const Vec& U) override;

private:
	//inline auto Eview() const { return make_sketch_view(proj, E); }
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
	double epsilon = 1.E-13; 	///< accuracy for hyperbola distance
	double T;					///< threshold
	int xihat;					///< cached for case T>0
	double u, v;				///< cached for case T<=0

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
		@param _geq a boolean, designating an upper (when false) or lower (when true) bound
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
	A safe zone function for condition Tlow <= dot_est(X,Y) <= Thigh.

	Let \f$ X = [X_1, ... , X_d]\f$ be am AGMS sketch of depth \f$d\f$.

	Given sketches \f$X \f$ and \f$Y\f$, the join estimate is 
	\f[ Q(X,Y) =  \mathop{\mathrm{median}}\{X_iY_i \,|\, i=1,\ldots , D\}. \f]
	Given thresholds \f$ T_\text{low} \f$ and \f$ T_\text{high} \f$, 
	the admissible region is 
    \f[ A =\{ (X,Y) | T_\text{low} \leq Q(X,Y) \leq T_\text{hi} \}. \f]

    The safe zone is defined per the algorithms of [Garofalakis and Samoladas, 
    ICDT 2017].
  */
struct twoway_join_agms_safezone : safezone_func
{
	twoway_join_agms_safezone();

	/**
		\brief Holds data for constraint of the form \f$ x^2 - y^2 \geq T \f$.

		Essentially, this object computes the ELM safe zone function for constraint
		\f[  \mathop{\mathrm{median}}\limits_{i=1,\ldots,d} \{ X_i^2 - Y_i^2 \}  \geq T  \f]
		where the input is a concatenation of sketches \f$ X, Y\f$ of depth \f$d \f$.

		However, since it is intended as an internal helper for \c twoway_join_agms_safezone,
		the API does not follow the normal function object conventions.
	 */
	struct bound {
		projection proj;		/// The projection
		double T;				/// The threshold
		Vec hat;				/// \f$ \hat{\xi}\f$
		vector<bilinear_2d_safe_zone> zeta_2d; /// d-array of 2-dimensional bilinear safe zones
		quorum_safezone Median;	/// median quorum

		/**
			Incremental state.

			These are the incremental states for \c dot_estvec_inc.
		  */
		struct incremental_state {
			Vec x2, y2;
		};

		/// Used to implement uninitialized objects
		bound();

		/// Initialize properly
		bound(const projection& _proj, double _T, bool eikonal);

		/// Called during initialization
		void setup(const Vec& norm_xi, const Vec& norm_psi);

		/// Compute from scratch with incstate initialization
		double zeta(incremental_state& inc, const Vec& x, const Vec& y);

		/// Compute incrementally
		double zeta(incremental_state& inc, const delta_vector& dx, const delta_vector& dy);

		/// Computes the safezone of the median of the 2-d safe zone functions
		double zeta(const Vec& x2, const Vec& y2);
	};

	size_t D;			//< The sketch size
	bound lower, upper;	//< The bounds objects

	/**
		Construct a safe zone function object.

		@param E is the reference point, which is the concatenation of two sketches.
		@param proj the sketch projection
		@param Tlow the lower bound
		@param Thigh the upper bound
		@param eikonal select an eikonal or non-eikonal (faster) function
	  */
	twoway_join_agms_safezone(const Vec& E, const projection& proj, 
								double Tlow, double Thigh, bool eikonal);

	/// Move assignment
	twoway_join_agms_safezone& operator=(twoway_join_agms_safezone&&)=default;


	struct incremental_state
	{ 
		/// these are used for incremental polarization
		Vec x,y;
		/// These are used for the bounds 
		struct bound::incremental_state lower, upper;
	};


	/**
		\brief Return the safe zone function value, computed from-scratch.

		@param U the concatenation of two sketches
		@return the value \f$\zeta(U)\f$
	  */
	double operator()(const Vec& U);

	/**
		\brief Return the safe zone function value, computed from-scratch initializing 
		incremental state.

		@param inc the incremental state to initialize
		@param U the concatenation of two sketches
		@return the value \f$\zeta(U)\f$
	  */
	double with_inc(incremental_state& inc, const Vec& U);

	/**
		\brief Return the safe zone function value, computed from-scratch initializing 
		incremental state.

		@param inc the incremental state to initialize
		@param DX the delta_vector of the concatenation of two sketches
		@return the value \f$\zeta(U)\f$
	  */
	double inc(incremental_state& incstate, const delta_vector& DX);

};


struct twoway_join_query_state : query_state
{
	projection proj;	/// the projection
	double beta;		/// the overall precision
	double epsilon;		/// the **assumed** sketch precision

	twoway_join_agms_safezone safe_zone;	/// the safe zone object

	twoway_join_query_state(double _beta, projection _proj);

    void update_estimate(const Vec& newE) override;
	virtual double query_func(const Vec& x) override;

	virtual void* alloc_incstate() override;
	virtual void free_incstate(void*) override;
	virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U) override;
	virtual double compute_zeta(void* inc, const Vec& U) override;

protected:
	void compute();

};






} // end namespace gm


#endif
