#ifndef __AGMS_HH__
#define __AGMS_HH__

#include <cassert>
#include <algorithm>
#include <valarray>


#include "dds.hh"


namespace agms {

using std::valarray;
using namespace std;


/// The index type
typedef size_t index_type;

/// The depth type
typedef unsigned int depth_type;

using key_type = dds::key_type;


typedef valarray<double> Vec;
typedef valarray<size_t> Index;
typedef valarray<bool> Mask;


/**
	Return the k-th order statistic from an array.
  */
template <typename T>
inline T order_select(int k, int n, T* ptr) {
        nth_element(ptr, ptr+k, ptr+n);
        return ptr[k];
}


/**
	@brief A hash family for AGMS sketches.

	An AGMS hash family defines the random projection
	of keys on the projected space.

	The depth \f$ D \f$ of the hash family is related
	to the number of projections.

	Using the hash family, one can use functions \c hash31 and
	\c fourwise to map a key \f$ x \f$ of type \c key_type to 
	an index \f$ z \f$ of type \c index_type.
  */
class hash_family
{
public:
	/// Construct a hash family of the given depth
	hash_family(depth_type D);

	~hash_family();

	/// Return the hash for a key
	inline long long hash(depth_type d, key_type x) const {
		assert(d<D);
		return hash31(F[0][d], F[1][d], x);
	}

	/// Return a 4-wise independent bit
	inline long long fourwise(depth_type d, key_type x) const {
		return hash31(hash31(hash31(x,F[2][d],F[3][d]), x, F[4][d]),x,F[5][d]);
	}

	/// Depth of the hash family
	inline depth_type depth() const { return D; }

protected:

	static inline long long hash31(int64_t a, int64_t b, int64_t x) {
		// use 64-bit arithmetic
		int64_t result = (a * x)+b;
		return ((result>>31)+result) & 2147483647ll;
	}

	depth_type D;
	int64_t * F[6];

public:
	static hash_family* get_cached(depth_type D);

};


/**
	An AGMS projection defines a projection 
	of a high-dimensional vector space on a
	specific sketch space.

	The epsilon error and the probability
	of error of estimates for the given projection
	are computed as per the Alon et al paper
  */
class projection
{
public:

	hash_family* const hf;
	const index_type L;

	inline depth_type depth() const { return hf->depth(); }
	inline index_type width() const { return L; }
	inline size_t size() const { return depth()*width(); }

	projection() : hf(0), L(0) {}

	projection(hash_family* _hf, index_type _L)
	: hf(_hf), L(_L)
	{ }

	projection(depth_type _D, index_type _L)
	: hf(hash_family::get_cached(_D)), L(_L)
	{ }


	inline void update_index(key_type key, Index& idx) const
	{
		assert(idx.size()==depth());
		size_t stride = 0;
		for(size_t d=0; d<depth(); d++) {
			idx[d] = stride + hf->hash(d, key) % L;
			stride += width();
		}
	}

	inline void update_mask(key_type key, Mask& mask) const 
	{
		assert(mask.size()==depth());
		for(size_t d=0; d<depth(); d++) {
			mask[d] = hf->fourwise(d, key);
		}		
	}

	inline slice S(size_t d) const { return slice(d*width(), width(),1); }

	inline bool operator==(const projection& p) const {
		return hf==p.hf && width()==p.width();
	}

	inline bool operator!=(const projection& p) const {
		return !(*this == p);
	}


	/** Sketch performance bounds according to Alon et al. */
	inline double epsilon() const { return 4./sqrt(L); }
	inline double prob_failure() const { return pow(1./sqrt(2.), depth()); }

};


/**
	AGMS sketch.

	This sketch is a fast version of the well-known AMS sketch.

	An AGMS sketch is associated with a hash family, of depth
	\f$ D \f$. It is a collection of \f$D\f$ vectors, of dimension
	\f$ L \f$ each. Thus, it can be thought of as a \f$D\times L\f$
	matrix, or alternatively, as a (composite) vector of dimension
	\f$D\cdot L\f$.

	An agms sketch supports all basic vector operations.
 */
class sketch : public Vec
{
public:

	/// Initialize to a null sketch
	inline sketch() {}

	/// Initialize by a given projection
	inline sketch(const projection& _proj)
	: Vec(_proj.size()), proj(_proj) {}

	/// Initialize to a zero sketch
	inline sketch(hash_family* _hf, index_type _L)
	: sketch(projection(_hf,_L))
	{  }

	/// Initialize to a zero sketch
	inline sketch(depth_type _D, index_type _L) 
	: sketch(projection(_D,_L))
	{ }

	using Vec::operator=;

	/// The hash family
	inline hash_family* hashf() const { return proj.hf; }

	/// The projection
	inline const projection& projectn() const { return proj; }

	/// The dimension \f$L\f$ of the sketch.
	inline index_type width() const { return proj.width(); }

	/// The depth \f$D \f$ of the sketch.
	inline depth_type depth() const { return proj.depth(); }

	/// Update the sketch.
	void update(key_type key, double freq = 1.0);

	/// Insert a key into the sketch
	inline void insert(key_type key) { update(key, 1.0); }

	/// Erase a key from the sketch
	inline void erase(key_type key) { update(key, -1.0); }

	/// Return true if this sketch is compatible to sk
	inline bool compatible(const sketch& sk) const {
		return proj == sk.proj;
	}

	inline double norm_squared() const {
		return ((*this)*(*this)).sum();
	}


protected:
	agms::projection proj;
};


/**
	A container for a single sketch and 
	data for for fast incremental updates.
  */
struct incremental_sketch
{
	sketch sk;  // the sketch

	// temporaries, used to avoid parameter passing
	Index idx;
	Mask mask;
	Vec delta;

	inline const projection& proj() { return sk.projectn(); }
	inline depth_type depth() { return proj().depth(); }
	inline depth_type width() { return proj().width(); }

	incremental_sketch(const projection& proj)
	: 	sk(proj), 
		idx(proj.depth()), 
		mask(proj.depth()), 
		delta(proj.depth())
	{ 	}

	void prepare_indices(key_type key)
	{
		proj().update_index(key, idx);
		proj().update_mask(key, mask);		
	}

	void update_counters(double freq)
	{
		delta[mask] = freq;
		delta[!mask] = -freq;
		sk[idx] += delta;
	}

	void update(key_type key, double freq=1.0) 
	{
		prepare_indices(key);
		update_counters(freq);
	}

	void insert(key_type key) { update(key,1.0); }
	void erase(key_type key) { update(key,-1.0); }
};


/**
	Can incrementally update the value of the 
	squared norm.
  */
struct incremental_norm2
{
	incremental_sketch* isk;
	Vec cur_norm2;

	incremental_norm2(incremental_sketch* _isk)
	: isk(_isk), cur_norm2(_isk->depth())
	{
		update_directly();  // initializing
	}
		
	// init the cur norm2 vector
	void update_directly()
	{
		const size_t D = isk->depth();
		const size_t L = isk->width();
		sketch& sk = isk->sk;

		size_t off = 0;
		for(size_t d=0; d < D; ++d) {
			double sum = 0.0;
			for(size_t i=0; i< L; i++) {
				double x = sk[off++];
				sum += x*x;
			}
			cur_norm2[d] = sum;
		}
	}

	/* 
		Note! the update has already been applied to
		the sketch. I.e., we now have available
		the value S' = S+delta.

		The update is taken by updating 
		cur_norm2  +=  2*delta*S' - delta^2
	*/
	void update_incremental()
	{
		Vec& delta = isk->delta;
		Vec& sk = isk->sk;
		Index& idx = isk->idx;
		// unoptimized!!!
		cur_norm2 += 2.*delta*sk[idx] - delta * delta;
	}

	double norm2_estimate() const {
		const depth_type D = isk->depth();
		assert(cur_norm2.size()==D);
		double est[D];
		copy(begin(cur_norm2), end(cur_norm2), est);
		return order_select(D/2, D, est);
	}

};


/**
	Can incrementally update the value of the 
	inner product.
  */
struct incremental_prod
{
	incremental_sketch *isk1, *isk2;
	Vec cur_prod;

	incremental_prod(incremental_sketch* sk1, incremental_sketch* sk2)
	: isk1(sk1), isk2(sk2), cur_prod(isk1->depth())
	{ 
		assert(isk1->sk.compatible(isk2->sk));
		update_directly();
	}

	void update_directly() 
	{
		sketch &sk1 = isk1->sk;
		sketch &sk2 = isk2->sk;
		const size_t D = sk1.depth();
		const size_t L = sk1.width();

		size_t off = 0;
		for(size_t d=0; d < D; ++d) {
			double sum = 0.0;
			for(size_t i=0; i< L; i++) {
				double x = sk1[off]*sk2[off];
				sum += x;
				off++;
			}
			cur_prod[d] = sum;
		}
	}	

	/*
		Just  cur_prod += U.delta * V.sk[idx];
	 */
	void _update_incremental(incremental_sketch& U, incremental_sketch& V)
	{
		Vec& delta = U.delta;
		Vec& sk = V.sk;
		Index& idx = U.idx;
		cur_prod += 1.*delta * sk[idx]; // BRAINDEAD!!!! without 1.*...
	}

	// call on lhs update
	void update_incremental_1()
	{
		_update_incremental(*isk1, *isk2);
	}

	// call on rhs update
	void update_incremental_2()
	{
		_update_incremental(*isk2, *isk1);
	}
};



/**
	Addition of two AGMS sketches.
  */
inline sketch operator+(const sketch& s1, const sketch& s2)
{
	assert(s1.compatible(s2));
	sketch result = s1;
	result += s2;
	return result;
}

}  // end namespace agms


/*
	Make agms::projection hashable
 */

namespace std
{
	using namespace agms;
    template <>
    struct hash<projection>
    {
        size_t operator()( const projection& p ) const;
    };
}




#endif