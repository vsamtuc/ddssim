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
	if(_D==0) throw std::domain_error("0 depth in hash family");

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

//
// Return a 31-bit random hash
//
inline int64_t hash31(int64_t a, int64_t b, int64_t x) {
	// use 64-bit arithmetic
	int64_t result = (a * x)+b;
	return ((result>>31)^result) & 2147483647ll;
}

long long hash_family::hash(depth_type d, key_type x) const {
	assert(d<D);
	return hash31(F[0][d], F[1][d], x);
}

/// Return a 4-wise independent bit
bool hash_family::fourwise(depth_type d, key_type x) const {
	return 
		hash31(hash31(hash31(x,F[2][d],F[3][d]), x, F[4][d]),x,F[5][d]) 
		& (1<<15);
}



void projection::update_index(key_type key, Index& idx) const
{
	assert(idx.size()==depth());
	size_t stride = 0;
	for(size_t d=0; d<depth(); d++) {
		idx[d] = stride + hf->hash(d, key) % L;
		stride += width();
	}
}

void projection::update_mask(key_type key, Mask& mask) const 
{
	assert(mask.size()==depth());
	for(size_t d=0; d<depth(); d++) {
		mask[d] = hf->fourwise(d, key);
	}		
}




void sketch::update(key_type key, double freq)
{
	hash_family* h = proj.hashf();
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


Vec agms::dot_estvec(const sketch& s1, const sketch& s2)
{
	assert(s1.compatible(s2));
	const depth_type D = s1.depth();
	Vec ret(D);

	for(size_t d=0;d<D;d++)
		ret[d] = std::inner_product(s1.row_begin(d), s1.row_end(d),
			s2.row_begin(d), 0.0);
	return ret;
}


Vec& agms::dot_estvec_inc(Vec& oldstate, const delta_vector& ds1, const sketch& s2)
{
	oldstate += (ds1.xnew - ds1.xold)*s2[ds1.index];
	return oldstate;
}


Vec& agms::dot_estvec_inc(Vec& oldstate, const sketch& s1, const delta_vector& ds2)
{
	return dot_estvec_inc(oldstate, ds2, s1);
}


Vec& agms::dot_estvec_inc(Vec& oldstate, const delta_vector& ds)
{
	oldstate += ds.xnew*ds.xnew - ds.xold*ds.xold;
	return oldstate;
}






size_t std::hash<projection>::operator()( const projection& p) const
{
	using boost::hash_value;
    using boost::hash_combine;

	size_t seed = 0;
	hash_combine(seed, hash_value(p.hashf()));
	hash_combine(seed, hash_value(p.width()));
	return seed;
}


isketch::isketch(const projection& _proj)
	: 	sketch(_proj), 
		delta(proj.depth()),
		mask(proj.depth())
	{ 	}



template <typename T>
void print_vec(const string& name, const T& a) 
{
	size_t n = a.size();
	cout << name << "[" << n << "]={";
	for(size_t i=0;i<n;i++)
		cout << (i?",":"") << a[i];
	cout << "}" << endl;
}


void isketch::update(key_type key, double freq)
{
	proj.update_index(key, delta.index);
	proj.update_mask(key, mask);
	
	delta.xold = (*this)[delta.index];
	for(size_t d=0; d<mask.size(); d++) {
		if(mask[d])
			delta.xnew[d] = delta.xold[d] + freq;
		else
			delta.xnew[d] = delta.xold[d] - freq;
	}
	
	(*this)[delta.index] = delta.xnew;
}

