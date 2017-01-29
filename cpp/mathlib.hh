#ifndef __MATHLIB_HH__
#define __MATHLIB_HH__

/**
	\file Data analytics and mathematical routines.
  */

#include <map>
#include <boost/numeric/ublas/vector.hpp>
#include <boost/numeric/ublas/vector_sparse.hpp>

namespace dds {

	template <typename Domain, typename Range=size_t>
	class distinct_histogram : public std::map<Domain, Range>
	{
	public:
		inline void add(const Domain& key, Range count=1) {
			auto loc = this->find(key);
			if(loc==this->end()) 
				this->insert(std::make_pair(key, count));
			else
				loc->second += count;
		}
	};

	using boost::numeric::ublas::map_std;
	using boost::numeric::ublas::mapped_vector;


	template <typename Domain, typename Range=size_t>
	class frequency_vector : public mapped_vector< Range, map_std<Domain, Range> >
	{
	public:
		typedef mapped_vector< Range, map_std<Domain, Range> > vector_type;

		using vector_type::mapped_vector;
		inline void add(const Domain& key, Range count=(Range)1) {
			(*this)(key) += count;
		}
	};


} // end namespace dds

#endif