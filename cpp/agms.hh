#ifndef __AGMS_HH__
#define __AGMS_HH__

#include <cassert>
#include <algorithm>

#include "dds.hh"
#include "mathlib.hh"

namespace agms {

using namespace std;


/// The index type
typedef size_t index_type;

/// The depth type
typedef unsigned int depth_type;

using key_type = dds::key_type;

using std::valarray;
typedef valarray<double> Vec;
typedef valarray<size_t> Index;
typedef valarray<bool> Mask;




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
	long long hash(depth_type d, key_type x) const;

	/// Return a 4-wise independent 
	bool fourwise(depth_type d, key_type x) const;

	/// Depth of the hash family
	inline depth_type depth() const { return D; }

protected:

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
	hash_family* hf;
	index_type L;

public:

	inline hash_family* hashf() const { return hf; }
	inline depth_type depth() const { return hf->depth(); }
	inline index_type width() const { return L; }
	inline size_t size() const { return depth()*width(); }

	inline projection() : hf(0), L(0) {}

	inline projection(hash_family* _hf, index_type _L)
	: hf(_hf), L(_L)
	{ }

	inline projection(depth_type _D, index_type _L)
	: hf(hash_family::get_cached(_D)), L(_L)
	{ }


	void update_index(key_type key, Index& idx) const;

	void update_mask(key_type key, Mask& mask) const; 


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

	An AGMS row will be a vector of size L. 

	An agms sketch supports all basic vector operations.
 */
class sketch : public Vec
{
public:
	/// the projection of the sketch
	agms::projection proj;

	/// Initialize to a null sketch
	inline sketch() {}

	/// Initialize by a given projection
	inline sketch(const projection& _proj)
	: Vec(0.0,_proj.size()), proj(_proj) 
	{ }

	/// Initialize to a zero sketch
	inline sketch(hash_family* _hf, index_type _L)
	: sketch(projection(_hf,_L))
	{ }

	/// Initialize to a zero sketch
	inline sketch(depth_type _D, index_type _L) 
	: sketch(projection(_D,_L))
	{ }

	sketch(const sketch&) = default;
	sketch(sketch&&) = default;

	//using Vec::operator=;
	inline sketch& operator=(const sketch& sk) = default;
	inline sketch& operator=(sketch&& sk) = default;
	inline sketch& operator=(const Vec& v) {
		if(v.size()!=size()) throw length_error("wrong vector size for sketch");
		this->Vec::operator=(v);
		return *this;
	}
	inline sketch& operator=(double v) { 
		this->Vec::operator=(v); return *this; 
	}
	inline sketch& operator=(const std::slice_array<double>& other) {
		this->Vec::operator=(other); return *this;
	}
	inline sketch& operator=(const std::gslice_array<double>& other) {
		this->Vec::operator=(other); return *this;
	}
	inline sketch& operator=(const std::mask_array<double>& other) {
		this->Vec::operator=(other); return *this;
	}
	inline sketch& operator=(const std::indirect_array<double>& other) {
		this->Vec::operator=(other); return *this;
	}


	/// The hash family
	inline hash_family* hashf() const { return proj.hashf(); }

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

	inline auto row_begin(size_t row) const {
		return begin(*this)+row*width();
	}

	inline auto row_end(size_t row) const {
		return begin(*this)+(row+1)*width();
	}

	inline double norm2_squared() const {
		return dds::dot(*this, *this);
	}

	inline size_t byte_size() const {
		return sizeof(float)*size();
	}
};



/**
	Return a vector of the dot products of 
	parallel rows of two sketches.
  */

Vec dot_estvec(const sketch& s1, const sketch& s2);


/**
	A shorthand for dot_estvec(s,s)
  */
inline Vec dot_estvec(const sketch& s) { 
	return dot_estvec(s,s); 
}


/**
	Return the (robust) estimate of the inner product
	by two agms sketches
  */
inline double dot_est(const sketch& s1, const sketch& s2)
{
	return dds::median(dot_estvec(s1,s2));
}

inline double dot_est(const sketch& sk)
{
	return dds::median(dot_estvec(sk));
}

// Vector space operations

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
inline sketch operator-(const sketch& s1, const sketch& s2)
{
	assert(s1.compatible(s2));
	sketch result = s1;
	result -= s2;
	return result;
}
inline sketch operator*(double a, const sketch& s)
{
	sketch result = s;
	result *= a;
	return result;
}
inline sketch operator*(const sketch& s, double a)
{
	sketch result = s;
	result *= a;
	return result;
}
inline sketch operator/(const sketch& s, double a)
{
	sketch result = s;
	result /= a;
	return result;
}



/**
	An incrementally updatable sketch.

	A container for a single sketch and data for fast incremental updates.
  */
struct isketch : sketch
{
	// temporaries, used to avoid parameter passing
	Index idx;
	Mask mask;
	Vec delta;

	isketch(const projection& proj);

	void prepare_indices(key_type key);
	void update_counters(double freq);

	inline void update(key_type key, double freq=1.0)
	{
		prepare_indices(key);
		update_counters(freq);
	}

	inline void insert(key_type key) { update(key,1.0); }
	inline void erase(key_type key) { update(key,-1.0); }
};




/**
	Base class for incremental_norm2 and incremental_prod
  */
struct incremental_est
{
	Vec row_est;

	incremental_est() {}
	incremental_est(size_t D) : row_est(D) {}
	incremental_est(const Vec& _v) : row_est(_v) {}
	incremental_est(Vec&& _v) : row_est(_v) {}

	inline double median_estimate() const {
		const depth_type D = row_est.size();
		return dds::order_select(D/2, row_est);
	}

};


/**
	Can incrementally update the value of the 
	squared norm.
  */
struct incremental_norm2 : incremental_est
{
	isketch* isk;

	incremental_norm2(isketch* _isk)
	: incremental_est(dot_estvec(*_isk)), isk(_isk)
	{ }
		
	// init the cur norm2 vector
	void update_directly()
	{
		row_est = dot_estvec(*isk);
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
		Index& idx = isk->idx;
		// unoptimized!!!
		row_est += 2.*delta*(*isk)[idx] - delta * delta;
	}

};


/**
	Can incrementally update the value of the 
	inner product.
  */
struct incremental_prod : incremental_est
{
	isketch *isk1, *isk2;

	incremental_prod(isketch* sk1, isketch* sk2)
	: isk1(sk1), isk2(sk2)
	{ 
		assert(isk1->compatible(*isk2));
		update_directly();
	}

	void update_directly() 
	{
		row_est = dot_estvec(*isk1, *isk2);
	}

	/*
		Just  cur_prod += U.delta * V.sk[idx];
	 */
	void _update_incremental(isketch& U, isketch& V)
	{
		Vec& delta = U.delta;
		Index& idx = U.idx;
		row_est += (+delta) * V[idx]; // BRAINDEAD!!!! without (+...), cannot do!
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