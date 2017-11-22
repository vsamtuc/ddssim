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

	This class is specialized by subclasses to access different safe zone functions.
	It abstracts safe zone computations by being a factory incremental state objects and
	providing access to a function. Also, it provides communication cost information to
	the middleware.
 */
struct safezone_func_wrapper
{
	virtual ~safezone_func_wrapper() { }

	/**
		\brief Return a new instance of the incremental state for this function.
	  */
	virtual void* alloc_incstate()=0;

	/**
		\brief Release a new instance of the incremental state for this function.
	  */
	virtual void free_incstate(void*)=0;

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
		Destructor.
	  */
	virtual ~query_state();

	/**
		\brief The dimension of the safe zone function space.

		This quantity represents the amount of data needed to
		describe the safe zone function. It is used in computing the
		network cost of transmitting the safe zone.

		Note that this is not related to the dimension of the (input)
		state vector.
	  */
	virtual size_t zeta_size() const override { return E.size(); }

	/// The query function
	virtual double query_func(const Vec& x)=0;

	/// The safezone function 
	virtual double zeta(const Vec& x) =0;

	/// Compute the safezone function given drift vector
	inline double compute_zeta(const Vec& U) {  return zeta(E+U); }

	/// Resetting the state
	virtual void update_estimate(const Vec& newE)=0;

	/// A functional interface
	inline double operator()(const Vec& x) { return query_func(x); }
};





} // end namespace gm




#endif
