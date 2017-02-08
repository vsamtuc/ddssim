#ifndef __MATHLIB_HH__
#define __MATHLIB_HH__

/**
	\file Data analytics and mathematical routines.
  */

#include <map>
#include <numeric>
#include <valarray>
#include <iostream>

#include <boost/numeric/ublas/vector.hpp>
#include <boost/numeric/ublas/vector_sparse.hpp>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/moment.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/rolling_variance.hpp>


namespace dds {

/*
	Vector functions based on valarray
 */

using std::valarray;

/** A real vector */
typedef valarray<double> Vec;

/** A vector of size_t, useful as an
    index to Vec
  */
typedef valarray<size_t> Index;

/** A vector of bool, useful as an
    index to Vec
  */
typedef valarray<bool> Mask;


/**
	Return the dot product of two Vec objects.
  */
template <typename T>
inline auto dot(const valarray<T>& a, const valarray<T>& b)
{
	return std::inner_product(begin(a),end(a),begin(b), (T)0);
}


/**
	Return the k-th order statistic from an array.
  */
template <typename T>
inline T order_select(int k, int n, T* ptr) {
        std::nth_element(ptr, ptr+k, ptr+n);
        return ptr[k];
}

/**
	Return the k-th order statistic of a Vec.
  */
double order_select(size_t k, Vec v);


/**
   Return the median of a Vec.
  */
double median(Vec v);

// Norms 
double norm_L1(const Vec& v);
double norm_L2(const Vec& v);
double norm_Linf(const Vec& v);


/**
	Return the relative error between an exact
	value and an estimate
  */
inline double relative_error(double exact, double estimate)
{
	return (exact==0.0)?
	 	(estimate==0.0 ? 0.0 : estimate)
	 	: fabs((exact-estimate)/exact);
}


/**
	A distinct histogram is used to hold a frequency
	count over a stream of observations.

	It is similar to \ref frequency_vector, but
	\c Domain need not be non-negative integers.
  */
template <typename Domain, typename Range=size_t>
class distinct_histogram : public std::map<Domain, Range>
{
public:
	inline Range& get_counter(const Domain& key) {
		auto loc = this->find(key);
		if(loc==this->end()) {
			return this->insert(std::make_pair(key, (Range)0)).first->second;
		}
		else {
			return loc->second;
		}		
	}

	inline Range add(const Domain& key) {
		return get_counter(key)++;
	}
	inline Range erase(const Domain& key) {
		return get_counter(key)--;
	}


};

using boost::numeric::ublas::map_std;
using boost::numeric::ublas::mapped_vector;


/**
	A frequency vector is used to hold a frequency
	count over a stream of numeric observations.

	The domain is restricted to the non-negative integers
	and the max. possible observation needs to be
	known in advance.

	This is a uBLAS vector, and can be used direcly with all
	the nice linear algebra routines in boost uBLAS.
  */
template <typename Domain, typename Range=size_t>
class frequency_vector 
	: public mapped_vector< Range, map_std<Domain, Range> >
{
public:
	typedef mapped_vector< Range, map_std<Domain, Range> > vector_type;

	using vector_type::mapped_vector;
	inline void add(const Domain& key, Range count=(Range)1) {
		(*this)(key) += count;
	}
};

using namespace boost::accumulators;

struct estimate_error_observer
{
protected:
public:
	using error_tally = 
		accumulator_set<double, 
			stats<tag::mean,tag::variance,
				tag::rolling_mean,tag::rolling_variance> 
			>;
	error_tally tally;

	/**
		Construct an observer with the given parameter for
		rolling window
	  */
	estimate_error_observer(size_t window);

	/**
		Add an error observation
	  */
	void observe(double exact, double est);
};




} // end namespace dds

#endif