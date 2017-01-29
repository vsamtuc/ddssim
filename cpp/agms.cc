#include <random>
#include <unordered_map>
#include "agms.hh"

using namespace std;


//---------------------------
// agms_hash_family methods
//---------------------------


static mt19937 engine(12344);

agms_hash_family::agms_hash_family(depth_type _D) : D(_D)
{
	assert(_D>0);
	assert(_D & 1);  // _D must be odd!

	uniform_int_distribution<key_type> U;

	for(size_t i=0; i<6; i++) {
		F[i] = new int64_t[_D];
		for(int d=0; d<_D; d++)
			F[i][d] = U(engine);
	}
}


agms_hash_family::~agms_hash_family()
{
	for(size_t i=0; i<6; i++)
		delete[] F[i];
}


// A cache type for hash families, which is cleared at destruction
struct agms_hf_cache 
	: unordered_map<agms_hash_family::depth_type, agms_hash_family*> 
{
	~agms_hf_cache() {
		for(auto x : *this) 
			delete x.second;
	}
};

// The hash family cache variable
static agms_hf_cache cache;


agms_hash_family* agms_hash_family::get_cached(depth_type D)
{
    if(cache.find(D)!=cache.end())
            return cache[D];
    else {
        agms_hash_family* ret = new agms_hash_family(D);
        cache[D] = ret;
        return ret;
	}
}



//---------------------------
// agms methods
//---------------------------



void agms::initialize(agms_hash_family* _hf, index_type _L)
{
	hf = _hf;
	L = _L;
	S = hf->depth()*L;
	counter = new double[S];
}

void agms::set_zero()
{
	fill(counter, counter+S, 0.0);	
}

agms::~agms()
{
	if(counter)
		delete[] counter;
}


void agms::update(key_type key, double freq)
{
	size_t D = depth();
	double* offset = counter;
	for(size_t d=0; d<D; d++) {
		double& loc = *(offset + hf->hash(d,key) % L);
		offset += L;
		if(hf->fourwise(d, key))
			loc += freq;
		else
			loc -= freq;
	}
}


