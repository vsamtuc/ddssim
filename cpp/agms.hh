#ifndef __AGMS_HH__
#define __AGMS_HH__

#include <cassert>
#include <algorithm>
#include <type_traits>

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
using dds::delta_vector;
using dds::Vec;
using dds::Index;
using dds::Mask;


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


template <typename IterType>
struct sketch_view;


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
	double eps;
public:

	inline hash_family* hashf() const { return hf; }
	inline depth_type depth() const { return hf->depth(); }
	inline index_type width() const { return L; }
	inline size_t size() const { return depth()*width(); }

	inline projection() : hf(0), L(0) {}

	inline projection(hash_family* _hf, index_type _L)
	: hf(_hf), L(_L), eps(ams_epsilon())
	{ }

	inline projection(depth_type _D, index_type _L)
	: projection(hash_family::get_cached(_D),_L)
	{ }


	inline size_t hash(depth_type d, key_type key) const {
		return hf->hash(d,key) % L;
	}

	inline bool fourwise(depth_type d, key_type key) const {
		return hf->fourwise(d,key);
	}

	void update_index(key_type key, Index& idx) const;

	void update_mask(key_type key, Mask& mask) const; 


	inline bool operator==(const projection& p) const {
		return hf==p.hf && width()==p.width();
	}

	inline bool operator!=(const projection& p) const {
		return !(*this == p);
	}


	/** Sketch performance bounds according to Alon et al. */
	inline double epsilon() const { return eps; }
	inline void set_epsilon(double e) { eps=e; }
	inline double ams_epsilon() const { return 4./sqrt(L); }
	inline double prob_failure() const { return pow(1./sqrt(2.), depth()); }

	/**
		Return a sketch view of a range. 

		The iterators must satisfy the RandomAccessIterator concept of STL.
	  */
	template<typename Iter>
	sketch_view<Iter> operator()(Iter from, Iter to) const;

	/**
		Return a sketch view on a container. 

		The data range is obtained by calling \c std::begin and \c std::end.
	  */
	template<typename Container>
	auto operator()(Container&) const;

};



/**
	AGMS sketch view

	This template applies AGMS sketch operations on a generic 
	collection of counters, as defined by a range of random-access
	iterators.
  */
template <typename IterType>
struct sketch_view
{
	typedef IterType iterator;
	typedef typename std::decay <decltype(* iterator())>::type counter_type;

	/// the projection of the sketch
	agms::projection proj;

	/// The range of the sketch
	IterType __begin, __end;

	sketch_view() {}

	sketch_view(const projection& _proj) : proj(_proj) { }
	sketch_view(const projection& _proj, const iterator& b, const iterator& e)
		: proj(_proj), __begin(b), __end(e) 
	{ 
		assert(valid_range(__begin, __end));
	}

	inline void set_range(const iterator& b, const iterator& e) {
		assert(valid_range(b, e));
		__begin = b;
		__end = e;
	}

	inline bool valid_range(const iterator& b, const iterator& e) const {
		return std::distance(b,e) == (ptrdiff_t) proj.size();
	}

	/// The hash family
	inline hash_family* hashf() const { return proj.hashf(); }

	/// The dimension \f$L\f$ of the sketch.
	inline index_type width() const { return proj.width(); }

	/// The depth \f$D \f$ of the sketch.
	inline depth_type depth() const { return proj.depth(); }

	/// The size of the projection which is also the size of the range
	inline size_t size() const { return proj.size(); }

	/// Return true if this sketch is compatible to sk
	template <typename Iter>
	inline bool compatible(const sketch_view<Iter>& sk) const {
		return proj == sk.proj;
	}

	inline iterator begin() const { return __begin; }

	inline iterator end() const { return __end; }

	inline iterator row_begin(size_t row) const {
		return __begin+(row*width());
	}

	inline iterator row_end(size_t row) const {
		return __begin+((row+1)*width());
	}

	inline counter_type operator[](size_t i) const { return __begin[i]; }
	inline counter_type& operator[](size_t i) { return __begin[i]; }

	/// Update the counters for key and freq
	void update(key_type key, counter_type freq = 1) const
	{
		hash_family* const h = proj.hashf();
		size_t off = 0;
		for(size_t d=0; d<depth(); d++) {
			size_t off2 = h->hash(d, key) % width();
			if(h->fourwise(d, key))
				__begin[off+off2] += freq;
			else
				__begin[off+off2] -= freq;
			off += width();
		}		
	}

	/// Update the counters for key and freq and set delta vector
	void update(delta_vector& delta, key_type key, counter_type freq = 1) const
	{
		proj.update_index(key, delta.index);
		for(size_t d=0; d<depth(); d++) {
			delta.xold[d] = __begin[delta.index[d]];
			__begin[delta.index[d]] += proj.fourwise(d, key) ? freq : -freq;
			delta.xnew[d] = __begin[delta.index[d]];
		}
	}

	/// Update the counters from index and mask, as returned by projection
	void apply_update(const Index& idx, const Mask& m, counter_type freq=1)  const
	{
		assert(idx.size() == m.size());
		for(size_t i=0;i<idx.size();i++) {
			__begin[idx[i]] += m[i]? freq : -freq;
		}
	}

	/// update the counters from delta vector
	void apply_update(const delta_vector& delta) const
	{
		for(size_t i=0; i<delta.index.size(); i++) {
			__begin[delta.index[i]] += delta.xnew[i]-delta.xold[i]; 
		}
	}

};



/**
	Sketch view over a vector
  */
typedef sketch_view<decltype(begin((Vec&) (* (Vec*)0) ))> Vec_sketch_view;
typedef sketch_view<decltype(begin((const Vec&) Vec()))> const_Vec_sketch_view;


/**
	Return a sketch view on a range. 
  */
template<typename Iter>
inline sketch_view<Iter> projection::operator()(Iter from, Iter to) const
{
	return sketch_view<Iter>(*this, from, to);
}

/**
	Return a sketch view on a container
  */
template<typename Container>
auto projection::operator()(Container& c) const
{
	typedef decltype(std::begin(c)) iter;
	return sketch_view<iter>(*this, std::begin(c), std::end(c));
}



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
	inline sketch() { }

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

	template <typename Iter>
	inline sketch(const sketch_view<Iter>& skv)
	: Vec(skv.size()), proj(skv.proj) 
	{
		std::copy(skv.begin(), skv.end(), begin(*this));
	}


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

	template <typename Iter>
	inline sketch& operator=(const sketch_view<Iter>& skv) {
		if(proj == skv.proj) 
			std::copy(skv.begin(), skv.end(), std::begin(*this));
		else
			*this = sketch(skv);
		return *this;
	}

	/// The hash family
	inline hash_family* hashf() const { return proj.hashf(); }

	/// The dimension \f$L\f$ of the sketch.
	inline index_type width() const { return proj.width(); }

	/// The depth \f$D \f$ of the sketch.
	inline depth_type depth() const { return proj.depth(); }

	inline auto view() { 
		return Vec_sketch_view(proj, begin(*this), end(*this)); 
	}
	inline auto view() const { 
		return const_Vec_sketch_view(proj, begin(*this), end(*this)); 
	}

	inline operator Vec_sketch_view() { return view(); }

	/// Update the sketch.
	inline void update(key_type key, double freq = 1.0) { view().update(key, freq); }

	inline void update(delta_vector& delta, key_type key, double freq = 1.0)
	{ view().update(delta, key, freq); }

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

template <typename Iter>
Vec dot_estvec(const sketch_view<Iter>& s1, const sketch_view<Iter>& s2)
{
	assert(s1.compatible(s2));
	const depth_type D = s1.depth();
	Vec ret(D);

	for(size_t d=0;d<D;d++)
		ret[d] = std::inner_product(s1.row_begin(d), s1.row_end(d),
			s2.row_begin(d), 0.0);
	return ret;
}


/**
	Incremental version of `dot_estvec`. 

	The incremental state is the previous result. 
	Therefore this function takes it as its first argument (by reference),
	mutates it and returns a reference to it.

	@see dot_estvec
  */
template <typename Iter>
inline Vec& dot_estvec_inc(Vec& oldvalue, const delta_vector& ds1, const sketch_view<Iter>& s2)
{
	for(size_t i=0; i<ds1.index.size(); i++)
		oldvalue[i] += (ds1.xnew[i] - ds1.xold[i])*s2[ds1.index[i]];
	return oldvalue;
}


/**
	Incremental version of `dot_estvec`. 

	The incremental state is the previous result. 
	Therefore this function takes it as its first argument (by reference),
	mutates it and returns a reference to it.

	@see dot_estvec
  */
template <typename Iter>
inline Vec& dot_estvec_inc(Vec& oldvalue, const sketch_view<Iter>& s1, const delta_vector& ds2)
{
	return dot_estvec_inc(oldvalue, ds2, s1);
}


/**
	A shorthand for dot_estvec(s,s)
  */
template <typename Iter>
inline Vec dot_estvec(const sketch_view<Iter>& s) 
{ 
	return dot_estvec(s,s); 
}


/**
	Incremental version for dot_estvec(s).

	The incremental state is just the previous value.
  */
inline Vec& dot_estvec_inc(Vec& oldvalue, const delta_vector& ds)
{
	oldvalue += ds.xnew*ds.xnew - ds.xold*ds.xold;
	return oldvalue;
}


/**
	Return the (robust) estimate of the inner product
	of two agms sketches
  */
template <typename Iter>
inline double dot_est(const sketch_view<Iter>& s1, const sketch_view<Iter>& s2)
{
	return dds::median(dot_estvec(s1,s2));
}

/**
	Return the (robust) estimate of the self product
	of an agms sketch
  */
template <typename Iter>
inline double dot_est(const sketch_view<Iter>& sk)
{
	return dds::median(dot_estvec(sk));
}


/**
	Return the (robust) estimate of the inner product
	of two agms sketches and the incremental state (which is overwritten)
  */
template <typename Iter>
inline double dot_est_with_inc(Vec& incstate, const sketch_view<Iter>& s1, const sketch_view<Iter>& s2)
{
	incstate.resize(s1.depth());
	incstate = dot_estvec(s1,s2);
	return dds::median(incstate);
}


template <typename Iter>
inline double dot_est_inc(Vec& incstate, const delta_vector& ds1, const sketch_view<Iter>& s2)
{
	return dds::median(dot_estvec_inc(incstate, ds1,s2));
}

template <typename Iter>
inline double dot_est_inc(Vec& incstate, const sketch_view<Iter>& s1, const delta_vector& ds2)
{
	return dds::median(dot_estvec_inc(incstate, s1, ds2));
}



/**
	Return the (robust) incremental estimate of the inner product
	of two agms sketches
  */
template <typename Iter>
inline double dot_est_with_inc(Vec& incstate, const sketch_view<Iter>& sk)
{
	incstate.resize(sk.depth());
	incstate = dot_estvec(sk);
	return dds::median(incstate);
}


inline double dot_est_inc(Vec& incstate, const delta_vector& dsk)
{
	return dds::median(dot_estvec_inc(incstate, dsk));
}


//
// Inner product
//

inline Vec dot_estvec(const sketch& s1, const sketch& s2) { return dot_estvec(s1.view(), s2.view()); }
inline Vec& dot_estvec_inc(Vec& inc, const delta_vector& ds1, const sketch& s2) { 
	return dot_estvec_inc(inc,ds1,s2.view()); }
inline Vec& dot_estvec_inc(Vec& inc, const sketch& s2, const delta_vector& ds1) { 
	return dot_estvec_inc(inc,ds1,s2.view()); }


inline double dot_est(const sketch& s1, const sketch& s2) { return dot_est(s1.view(), s2.view()); }
inline double dot_est_with_inc(Vec& incstate, const sketch& s1, const sketch& s2) {
 	return dot_est_with_inc(incstate, s1.view(), s2.view()); }
inline double dot_est_inc(Vec& incstate, const delta_vector& s1, const sketch& s2) {
 	return dot_est_inc(incstate, s1, s2.view()); }
inline double dot_est_inc(Vec& incstate, const sketch& s1, const delta_vector& s2) {
 	return dot_est_inc(incstate, s1.view(), s2); }


//
// Self-product
//

inline Vec dot_estvec(const sketch& s) { return dot_estvec(s.view()); }

inline double dot_est(const sketch& s) { return dot_est(s.view()); }
inline double dot_est_with_inc(Vec& incstate, const sketch& s) {
 	return dot_est_with_inc(incstate, s.view()); }



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
	A reference to a sketch to perform incremental updates.

	A container for a sketch reference and a delta_vector. 
	The update method also updates the delta.
  */
struct inc_sketch_updater
{
	sketch& sk;
	delta_vector delta;
private:
	// temporary, used to avoid parameter passing
	Mask mask;
public:

	inc_sketch_updater(sketch& _sk);

	void update(key_type key, double freq=1.0);
	inline void insert(key_type key) { update(key,1.0); }
	inline void erase(key_type key) { update(key,-1.0); }
};


/**
	An incrementally updatable sketch.

	A container for a single sketch and a delta_vector. 
	The update method also updates the delta.
  */
struct isketch : sketch
{
	delta_vector delta;
private:
	// temporary, used to avoid allocation
	Mask mask;
public:

	isketch(const projection& proj);

	void update(key_type key, double freq=1.0);
	inline void insert(key_type key) { update(key,1.0); }
	inline void erase(key_type key) { update(key,-1.0); }
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