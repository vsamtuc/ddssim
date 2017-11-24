#ifndef __SZ_QUORUM_HH__
#define __SZ_QUORUM_HH__

#include "hdv.hh"

namespace gm {

using namespace hdv;

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
struct quorum_safezone
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


	double zeta_eikonal(const Vec& zX);
	double zeta_non_eikonal(const Vec& zX);

	inline double operator()(const Vec& zX) {
		return (eikonal) ? zeta_eikonal(zX) : zeta_non_eikonal(zX);
	}

};


} // end namespace gm


#endif

