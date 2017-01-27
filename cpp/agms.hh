#ifndef __AGMS_HH__
#define __AGMS_HH__

#include <cassert>
#include <algorithm>

#include "dds.hh"

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
class agms_hash_family
{
public:
	/// The key type
	typedef dds::key_type key_type;

	/// The index type
	typedef int32_t index_type;

	/// The depth type
	typedef int depth_type;

	/// Construct a hash family of the given depth
	agms_hash_family(depth_type D);

	~agms_hash_family();

	/// Return the hash for a key
	inline index_type hash(depth_type d, key_type x) const {
		assert(d<D);
		return hash31(F[0][d], F[1][d], x);
	}

	/// Return a 4-wise independent bit
	inline index_type fourwise(depth_type d, key_type x) const {
		return hash31(hash31(hash31(x,F[2][d],F[3][d]), x, F[4][d]),x,F[5][d]);
	}

	/// Depth of the hash family
	inline depth_type depth() const { return D; }

protected:
	static inline index_type hash31(key_type a, key_type b, key_type x) {
		key_type result = (a*x)+b;
		return ((result>>31)+result) & 2147483647ll;
	}

	depth_type D;
	key_type * F[6];

public:
	static agms_hash_family* get_cached(depth_type D);

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
class agms
{
public:
	/// The index type
	typedef agms_hash_family::index_type index_type;

	/// The key type
	typedef agms_hash_family::key_type key_type;

	/// The depth type
	typedef agms_hash_family::depth_type depth_type;

	/// Initialize to a null sketch
	inline agms() : hf(0), L(0), S(0), counter(0) {}

	/// Initialize to a zero sketch
	inline agms(agms_hash_family* _hf, index_type _L) {
		initialize(_hf, _L);
		set_zero();
	}

	/// Initialize to a zero sketch
	inline agms(depth_type _D, index_type _L) {
		initialize(agms_hash_family::get_cached(_D), _L);
		set_zero();
	}

	/// Move constructor
	inline agms(agms&& sk) {
		hf = sk.hf;
		L = sk.L;
		S = sk.S;
		counter = sk.counter; 
		sk.counter = 0;
	}

	/// Copy constructor
	inline agms(const agms& sk) {
		initialize(sk.hf, sk.L);
		if(sk.initialized()) 
			std::copy(sk.counter, sk.counter+S, counter);
	}

	~agms();

	/// The hash family for this sketch
	inline agms_hash_family* hash_family() const { return hf; }

	/// The dimension \f$L\f$ of the sketch.
	inline index_type width() const { return L; }

	/// The depth \f$D \f$ of the sketch.
	inline depth_type depth() const { return hf->depth(); }

	/// The vector size of the sketch
	inline size_t size() const { return S; }

	/// Return true if the sketch is not initialized
	inline bool initialized() const { return hf != 0; }

	/// Return the beginning of the vector array
	inline double* begin() { return counter; }

	/// Return the end of the vector array
	inline double* end() { return counter+S; }

	/// Update the sketch
	void update(key_type key, double freq = 1.0);

	/// Insert a key into the sketch
	inline void insert(key_type key) { update(key, 1.0); }

	/// Erase a key from the sketch
	inline void erase(key_type key) { update(key, -1.0); }

	/// Return true if this sketch is compatible to sk
	inline bool compatible(const agms& sk) const {
		return hf == sk.hf && L == sk.L;
	}

	/// Assignment
	inline agms& operator=(const agms& sk) {
		if(!initialized()) {
			initialize(sk.hf, sk.L);
		} else {
			assert(compatible(sk));
		}
		std::copy(sk.counter, sk.counter+S, counter);
		return *this;
	}


	/// Constant assignment
	inline agms& operator=(double x) {
		assert(initialized());
		std::fill(counter, counter+S, x);
		return *this;
	}


	/// Move assignment
	inline agms& operator=(agms&& sk) {
		using std::swap;
		swap(hf, sk.hf);
		swap(counter, sk.counter);
		swap(L, sk.L);		
		swap(S, sk.S);		
		return *this;
	}


	inline agms& operator += (const agms& sk) {
		assert(compatible(sk));
		for(index_type i=0; i<S; i++) 
			counter[i] += sk.counter[i];
		return *this;
	}


	inline agms& operator *= (double a) {
		assert(initialized());
		for(index_type i=0; i<S; i++) 
			counter[i] *= a;
		return *this;
	}


	inline double norm_squared() const {
		assert(initialized());
		double s = 0.0;
		for(index_type i=0; i<S; i++) {
			double x = counter[i];
			s += x*x;
		}
		return s;
	}


protected:
	agms_hash_family* hf;
	index_type L, S;
	double* counter;

private:
	void initialize(agms_hash_family*, index_type);
	void set_zero();
};


/**
	Addition of two AGMS sketches.
  */
inline agms operator+(const agms& s1, const agms& s2)
{
	agms result = s1;
	result += s2;
	return result;
}




#endif