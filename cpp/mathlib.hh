#ifndef __MATHLIB_HH__
#define __MATHLIB_HH__

/**
	\file Data analytics and mathematical routines.
  */

#include <map>

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



} // end namespace dds

#endif