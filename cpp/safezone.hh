#ifndef __SAFEZONE_HH__
#define __SAFEZONE_HH__


#include <iomanip>

#include "mathlib.hh"
#include "agms.hh"


/**
	This file contains computational code for defining admissible
	regions, safe zone functions, distance functions, etc.
  */

namespace dds { 

using namespace agms;



struct selfjoin_query
{
	double beta; 	// the overall precision
	sketch E;   	// The current estimate for the stream

	double Qest; 	// current estimate
	double Tlow;    // admissible region: Tlow <= Q(t) <= Thigh
	double Thigh;   // 


	selfjoin_query(double _beta, projection _proj)
	: beta(_beta), E(_proj)
	{
		assert(_proj.epsilon() < beta);
		Qest = 0.0;

		// initialize the current estimate to 0
		Tlow = Thigh = Qest = 0.0;
	}


	void update_estimate(const sketch& newE)
	{
		// compute the admissible region
		E = newE;
		Qest = dot_est(E);

		Tlow = (1+E.proj.epsilon())*Qest/(1.0+beta);
		Thigh = (1-E.proj.epsilon())*Qest/(1.0-beta);
	}
};



/**
	Base function for boolean quantile safezones.

	This class is used to prepare a safezone function
	for quantile queries, both eikonal and non-eikonal,
	by projecting out the components of the space 
	where the reference point is false.
  */
struct quantile_safezone
{
	size_t n;	/// the number of inputs
	Index L;  	/// the legal inputs index from n to zetaE
	Vec zetaE;	/// the reference vector's zetas, where zE >= 0.

	quantile_safezone() : n(0) { }

	quantile_safezone(const Vec& zE) 
	: n(zE.size())
	{
		// count legal inputs
		size_t Legal[n];
		size_t pos = 0;
		for(size_t i=0; i<n; i++)
			if(zE[i]>0) Legal[pos++] = i;

		// create the index and the other matrices
		Index Ltmp(Legal, pos);
		Ltmp.swap(L);
		zetaE.resize(L.size());
		zetaE = zE[L];
	}

	quantile_safezone(quantile_safezone&&) = default;
	quantile_safezone& operator=(quantile_safezone&&) = default;
};


/**
	The non-eikonal safezone function for a boolean quantile query.

	The quantile boolean function, \f$ Q_{k,n} (x_1, ... , x_n)\f$, 
	is the boolean function which is
	true iff at least \f$k,  1\leq k \leq n\f$, of the 
	\f$ x_i\f$ inputs are true.
	For \f$ k=1 \f$, this is equivalent to the "or" and
	for \f$ k=n \f$ this is equivalent to the "and".
	The resulting function is non-eikonal and can be
	computed in time \f$ O(n) \f$.

	@see quantile_safezone_eikonal
  */
struct quantile_safezone_non_eikonal : quantile_safezone
{
	size_t k;	// the lower bound on true inputs

	quantile_safezone_non_eikonal() { }

	quantile_safezone_non_eikonal(quantile_safezone&& qsz, size_t _k) 
	: k(_k) 
	{
		this->quantile_safezone::operator=(std::move(qsz));
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");		
	}

	quantile_safezone_non_eikonal(const Vec& zE, size_t _k) 
	: quantile_safezone(zE), k(_k)
	{
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");
	}

	double operator()(const Vec& zX) {
		Vec zEzX = zetaE;
		zEzX *= zX[L];
		std::nth_element(begin(zEzX), begin(zEzX)+(L.size()-k), end(zEzX));
		return std::accumulate(begin(zEzX), begin(zEzX)+(L.size()-k+1), 0.0);
	}

};


/**
	The eikonal safezone function for a boolean quantile query.

	The quantile boolean function, 
		\f[ Q_{k,n} (x_1, ... , x_n),\f]
	is the boolean function which is
	true iff at least \f$k,  1\leq k \leq n\f$, of the 
	\f$ x_i\f$ inputs are true.
	For \f$ k=1 \f$, this is equivalent to the "or" and
	for \f$ k=n \f$ this is equivalent to the "and".

	The resulting function is eikonal and will be
	computed in time \f$ O(\binom{|L|}{k-1}) \f$.
  */
struct quantile_safezone_eikonal : quantile_safezone
{
	size_t k;	/// the quantile

	quantile_safezone_eikonal() { }

	/// Construct by moving a quantile safezone
	quantile_safezone_eikonal(quantile_safezone&& qsz, size_t _k) 
	: k(_k) 
	{
		this->quantile_safezone::operator=(std::move(qsz));
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");
	}

	/**
		 Construct the function for given reference vector and \f$k\f$
	 */
	quantile_safezone_eikonal(const Vec& zE, size_t _k) 
	: quantile_safezone(zE), k(_k)
	{
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");
	}


	double operator()(const Vec& zX) {
		Vec zEzX = zetaE;
		zEzX *= zX[L];
		Vec zEzE = zetaE*zetaE;

		// find the minimum over all m-sets from [0:L.size())
		size_t l = L.size();
		size_t m = L.size()-k+1;

		size_t I[m];
		std::iota(I, I+m, 0);

		double zinf = zeta_I(I, m, zEzX, zEzE);
		while(next_I(I, m, l)) {
			double zI = zeta_I(I, m, zEzX, zEzE);
			zinf = std::min(zI, zinf);
		}

		return zinf;
	}
	
private:
	inline static double zeta_I(size_t *I, size_t m, const Vec& zEzX, const Vec& zEzE)
	{
		double num = 0.0;
		double denom = 0.0;
		for(size_t i=0; i<m; i++) {
			num += zEzX[I[i]];
			denom += zEzE[I[i]];
		}
		return num/sqrt(denom);
	}

	/**
		Helper to iterate over all strictly increasing m-long
		sequences over [0:l)
	  */
	inline static bool next_I(size_t *I, size_t m, size_t l) 
	{
		for(size_t i=1; i<=m; i++) {
			if(I[m-i] < l-i) {
				I[m-i]++;
				for(size_t j=1;j<i;j++) 
					I[m-i+j] = I[m-i]+j;
				return true;
			}
		}
		return false;
	}


};



/**
	The safe zone function for the selfjoin estimate of a sketch.

	The admissible region is \f$ A =\{ X | \median{X_i^2}  \leq T \}\f$.

	The safe zone is defined as follows: 
	zeta_i(U_i)  =   sqrt(T) - ||E_i+U_i||

	zeta = inf_{I in (D choose K)}  [ \sum_{i\in I} zeta_i(0)*\zeta_i(U_i) ]

	TODO: Make it incremental! Current complexity is O(|sketch|)
  */
struct selfjoin_agms_safezone_upper_bound
{
	sketch& E;		// reference vector
	double T;		// threshold above

	quantile_safezone_eikonal Median;


	selfjoin_agms_safezone_upper_bound(sketch& _E, double _T)
	: 	E(_E), T(_T), 
		Median(sqrt(T) - sqrt(dot_estvec(E)), (E.depth()+1)/2)
	{ }

	inline double operator()(const sketch& U) 
	{
		Vec z = sqrt(T) - sqrt(dot_estvec(E+U));
		return Median(z);
	}
};



/**
	The safe zone function for the selfjoin estimate of a sketch.

	The admissible region is \f$ A =\{ X | \median{X_i^2}  \geq T \}\f$.

	The safe zone is defined as follows: 
	zeta_i(U_i)  =   U \frac{E}{\norm{E}} + \norm{E} - \sqrt{T}

	zeta = inf_{I in (D choose K)}  [ \sum_{i\in I} zeta_i(0)*\zeta_i(U_i) ]
  */
struct selfjoin_agms_safezone_lower_bound
{
	sketch& E;		// reference vector
	double T;		// threshold above

	quantile_safezone_eikonal Median;  // used to compose the median
	Vec norm_E_estvec;	// cached for speed


	selfjoin_agms_safezone_lower_bound(sketch& _E, double _T)
	: 	E(_E), T(_T), 
		Median(sqrt(dot_estvec(E)) - sqrt(T), (E.depth()+1)/2),
		norm_E_estvec( sqrt(dot_estvec(E,E)) )
	{ }

	inline double operator()(const sketch& U) 
	{
		Vec z = dot_estvec(U,E)/norm_E_estvec + norm_E_estvec - sqrt(T);
		return Median(z);
	}
};




struct selfjoin_agms_safezone
{
	sketch E;					// reference vector E
	double Tlow, Thigh;

	selfjoin_agms_safezone_lower_bound lower_bound;	// Safezone for sk^2 >= Tlow
	selfjoin_agms_safezone_upper_bound upper_bound;	// Safezone for sk^2 <= Thigh

	selfjoin_agms_safezone(const sketch& _E, double _Tlow, double _Thigh)
	: E(_E), Tlow(_Tlow), Thigh(_Thigh),
		lower_bound(E, _Tlow),
		upper_bound(E, _Thigh)
	{}

	selfjoin_agms_safezone(selfjoin_query& q) 
	:  selfjoin_agms_safezone(q.E, q.Tlow, q.Thigh)
	{ }

	inline double operator()(const sketch& U) {
		return min(lower_bound(U), upper_bound(U));
	}
};


} // end namespace dds


#endif