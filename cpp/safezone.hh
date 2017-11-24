#ifndef __SAFEZONE_HH__
#define __SAFEZONE_HH__

#include "agms.hh"
#include "sz_quorum.hh"
#include "sz_bilinear.hh"


/**
	This file contains computational code for defining admissible
	regions, safe zone functions, distance functions, for queries
	on AGMS sketches
  */


namespace gm { 

using namespace hdv;
using namespace agms;


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
struct selfjoin_agms_safezone_upper_bound 
{
	double sqrt_T;		// threshold above
	projection proj;
	quorum_safezone Median;

	typedef Vec incremental_state;

	selfjoin_agms_safezone_upper_bound() {}

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
struct selfjoin_agms_safezone_lower_bound
{
	sketch Ehat;		// normalized reference vector
	double sqrt_T;			// threshold above

	quorum_safezone Median;  //  the median

	typedef Vec incremental_state;

	selfjoin_agms_safezone_lower_bound() {}

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
struct selfjoin_agms_safezone 
{
	selfjoin_agms_safezone_lower_bound lower_bound;	// Safezone for sk^2 >= Tlow
	selfjoin_agms_safezone_upper_bound upper_bound;	// Safezone for sk^2 <= Thigh

	struct incremental_state
	{
		selfjoin_agms_safezone_lower_bound::incremental_state lower;
		selfjoin_agms_safezone_upper_bound::incremental_state upper;		
	};

	selfjoin_agms_safezone() {}

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
	Two-way join queries (inner product)
 */



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
struct twoway_join_agms_safezone 
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



} // end namespace gm


#endif
