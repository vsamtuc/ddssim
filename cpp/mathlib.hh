#ifndef __MATHLIB_HH__
#define __MATHLIB_HH__

/**
	\file Data analytics and mathematical routines.
  */

#include <map>
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
	estimate_error_observer(size_t window) 
		: tally(tag::rolling_window::window_size = window)
		{}

	void observe(double est, double exact) {
		using namespace std;
		double err = (exact==0.0)?0.0: abs((est-exact)/exact);
		//cout << "err=" << err << endl;
		tally(err);
	}

	//void report(std::osteam&);

};




} // end namespace dds

#endif