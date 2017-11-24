#ifndef __GM_SZONE_HH__
#define __GM_SZONE_HH__

/**
	\file Geometric method safezone-related classes
  */

#include "hdv.hh"

namespace gm {

using namespace hdv;

/**
	Abstract base class for a safe zone function wrapper.

	This class is specialized by subclasses to wrap different safe zone functions.
	Its compute_zeta methods take as arguments drift vectors (NOT actual state vectors!).

	The class provides a factory for incremental state objects. Also, it returns the
	dimension of the function space, that is, the amount of data needed to
	construct the function. For example, for a ball safe zone, this is 1 (the radius).

	This is used to, e.g., compute the cost of transmitting the wrapped function over 
	the network.
 */
struct safezone_func
{
	virtual ~safezone_func();

	/**
		\brief Return a new instance of the incremental state for this function.
	  */
	virtual void* alloc_incstate()=0;

	/**
		\brief Release a new instance of the incremental state for this function.
	  */
	virtual void free_incstate(void*)=0;

	/**
		\brief Compute the function on a drift vector
	  */
	virtual double compute_zeta(const Vec& U)=0;

	/**
		\brief Compute the function on a drift vector and set incremental state.
	  */
	virtual double compute_zeta(void* inc, const Vec& U)=0;

	/**
		\brief Incrementally compute the function on a drift vector.

		Note that the drift vector is also given as an argument. In this way,
		if the function has no incremental version, it is still possible to
		compute the function.
	  */
	virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U)=0;

	/**
		\brief The dimension of the safe zone function space.

		This quantity represents the amount of data needed to
		describe the safe zone function. It is used in computing the
		network cost of transmitting the safe zone.

		Note that this is not related to the dimension of the (input)
		state vector.
	  */
	virtual size_t zeta_size() const = 0;
};




/**
	Base class for a query state object.

	A query state holds the current global estimate \f$ E \f$,
	provides the query function as a virtual method,
	and maintains the current accuracy levels.

	Also, it is a factory for safe zone wrapper objects.
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
		Destructor.
	  */
	virtual ~query_state();


	/// The query function
	virtual double query_func(const Vec& x)=0;

	/// The safezone function 
	virtual double zeta(const Vec& x) =0;

	/**
		\brief Compute the safezone function given drift vector. 

		This function simply returns the result of zeta(U+E).
	  */
	virtual double compute_zeta(const Vec& U);

	/**
		 Reset the state by adding \c dE to E.

		 After this function, the query estimate, bounds and safezone are
		 adjusted to the new estimate vector.
	  */
	virtual void update_estimate(const Vec& dE)=0;

	/**
	 	\brief Return a safezone_func for the safe zone function.

	 	The returned object shares state with this object.
	 	It is the caller's responsibility to delete the returned object, and do
	 	so before this object is destroyed.
	 */
	virtual safezone_func* safezone() =0;

	/**
	 	\brief Return a safezone_func for a low-parametric safe zone function.

	 	The returned object shares state with this object.
	 	It is the caller's responsibility to delete the returned object, and do
	 	so before this object is destroyed.

	 	This function may return \c nullptr. However, if it returns a non-null value,
	 	the wrapped function \f$\zeta_R\f$ should have the following attributes:
		- it should be dominated everywhere by the safezone \f$\zeta\f$.
		- it should be $\zeta_R(E)=\zeta(E)$.
		- the \c zeta_size() method should return a 'small' value (this is purposely vague,
		  but in practice it should be a small constant, in particular much smaller than
		  E.size()).

		The default implementation returns null.
	 */
	virtual safezone_func* radial_safezone();


	/// A functional interface to the query function
	inline double operator()(const Vec& x) { return query_func(x); }
};



/**
	Template for providing a standard implementation for safezone wrapping.

	This is provided for convenience.
  */
template <typename SZFunc, typename IncState = typename SZFunc::incremental_state >
struct std_safezone_func : safezone_func
{
	typedef SZFunc function_type;
	typedef IncState incremental_state;

	SZFunc& func;
	size_t zsize;
	const Vec& E;

	std_safezone_func(SZFunc& _func, size_t _zsize, const Vec& _E) 
		: func(_func), zsize(_zsize), E(_E)
	{ }

	virtual void* alloc_incstate() override 
	{
		return new incremental_state;
	}
	virtual void free_incstate(void* ptr) override 
	{
		delete static_cast<incremental_state*>(ptr);
	}
	virtual size_t zeta_size() const override 
	{
		return zsize;
	}
	virtual double compute_zeta(const Vec& U) {
		return func(U+E);
	}
	virtual double compute_zeta(void* inc, const Vec& U) {
		incremental_state* incstate = static_cast<incremental_state*>(inc);		
		return func.with_inc(*incstate, U+E);
	}
	virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U)
	{
		incremental_state* incstate = static_cast<incremental_state*>(inc);
		delta_vector DU = dU;
		DU += E;
		return func.inc(*incstate, DU);
	}
};





} // end namespace gm




#endif
