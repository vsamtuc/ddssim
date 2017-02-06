#include <random>
#include <unordered_map>
#include <cstdio>

#include <boost/functional/hash.hpp>

#include "agms.hh"

using namespace std;


//---------------------------
// hash_family methods
//---------------------------

using namespace agms;

static mt19937_64 engine;

hash_family::hash_family(depth_type _D) : D(_D)
{
	assert(_D>0);
	assert(_D & 1);  // _D must be odd!

	uniform_int_distribution<int64_t> U;

	for(size_t i=0; i<6; i++) {
		F[i] = new int64_t[_D];
		for(size_t d=0; d<_D; d++)
			F[i][d] = U(engine);
	}

#if 0  // printout the random projection seeds
	for(depth_type d=0; d<D; d++) {
		printf("%4d:",d);
		for(size_t i=0;i<6; i++) {
			printf(" %12ld", F[i][d]);
		}
		printf("\n");
	}
#endif
}


hash_family::~hash_family()
{
	for(size_t i=0; i<6; i++)
		delete[] F[i];
}


// A cache type for hash families, which is cleared at destruction
struct agms_hf_cache 
	: unordered_map<depth_type, hash_family*> 
{
	~agms_hf_cache() {
		for(auto x : *this) 
			delete x.second;
	}
};

// The hash family cache variable
static agms_hf_cache cache;


hash_family* hash_family::get_cached(depth_type D)
{
    if(cache.find(D)!=cache.end())
            return cache[D];
    else {
        hash_family* ret = new hash_family(D);
        cache[D] = ret;
        return ret;
	}
}



void sketch::update(key_type key, double freq)
{
	hash_family* h = proj.hf;
	size_t off = 0;
	for(size_t d=0; d<depth(); d++) {
		size_t off2 = h->hash(d, key) % width();
		if(h->fourwise(d, key))
			(*this)[off+off2] += freq;
		else
			(*this)[off+off2] -= freq;
		off += width();
	}
}

size_t std::hash<projection>::operator()( const projection& p) const
{
	using boost::hash_value;
    using boost::hash_combine;

	size_t seed = 0;
	hash_combine(seed, hash_value(p.hf));
	hash_combine(seed, hash_value(p.L));
	return seed;
}
