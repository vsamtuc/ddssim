#ifndef __SZ_BILINEAR_HH__
#define __SZ_BILINEAR_HH__

#include "hdv.hh"

namespace gm {

using namespace hdv;

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
struct bilinear_2d_safe_zone 
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


}  // end namespace gm

#endif