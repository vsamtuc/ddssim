#ifndef __MATHLIB_HH__
#define __MATHLIB_HH__

/**
	\file hdv.hh

	Mathematical routines for high-dimensional vectors.

	# API pattern for inremental computations

	Here, we introduce a framework
	for incremental computations. 
	Take a function like \c dot(x,y), on two large vectors.
	The ''from-scratch'' implementation is straightforward.
	However, assume that vector x changes in only a few positions.
	An incremental version of dot, given the change in `x`,
	returns the new value of dot much faster.
	We now describe the API pattern used for incremental versions of
	our functions.

	Let `F(X)` be a function on `X`, computing the "mathematical" function \f$ f(X)\f$. 
	The expression "F(X)" denotes the
	''from scratch'' computation. Its incremental version will take the form
	```
	    F_inc(S, DeltaX)
	```
	where `S` is some "incremental state" object, returned by some past
	invocation with input `Xold` (more on this below) and DeltaX is an object
	that describes the change from the old input, `Xold`, to the current input
	`Xnew`. Note that, usually, `Xnew` is not provided!
	The return value of `F_inc` is the value of \f$ f(X_{\text{new}}) \f$.

	### Incremental state

	The "incremental state" of a function may consist simply of the previous
	result, in which case the new "incremental state" is the return value of 
	`F_inc`, or it may be an arbitrary object, in which case it is passed to
	`F_inc` by reference and is updated.

	### Delta
	
	The type of Delta is usually a compressed representation of the change from
	`Xold` to `Xnew`. For example, the library provides the type `vector_delta`,
	a simple structure with three fields, `Vec xold`, ` Vec xnew` and `Index index`. 
	the semantics of this object are: 

	> Vectors `Xold` and `Xnew` are equal in every coordinate not in `index`. For
	> a coordinate `i` in `index`, `xold[i]==Xold[index[i]]` and `xnew[i]==Xnew[index[i]]`.

	## An example

	Let us return to the `dot(x,y)` function as an example. Its "incremental state"
	consists of just the function result. 
	~~~~~~
	// create some big vectors
	Vec X(1000);
	Vec Y(1000);
	X = ... // Lots of data
	Y = ... // Lots of data

	// compute the initial dot product
	double xy = dot(X, Y);

	// make a small change in X
	Index index { 2,114, 256 };
	delta_vector DX(index);
	DX.xold = X[index];
	X[index] = Vec { 0.0, 1, -10 };
	DX.xnew = X[index];

	// update the value of dot (X,Y)
	xy = dot_inc(xy, DX, Y);

	// check that all went well!
	assert( xy == dot(X,Y) );
	~~~~~~

	## Obtaining an incremental state in the general case

	When function `F` requires an incremental state that is not simply the previous 
	result, an additional routine must be provided, which performs the ``from scratch``
	computation but also records the incremental state for future incremental
	invocations. When this routine is required, it takes the form `F_with_inc(S, X)`.

	An example is the norm_L2(X) function, which returns the square root of `dot(X)`.
	To make this function incremental, we can use the `dot_inc` routine. This routine
	expects its previous result. 
	Of course, it is possible to use the previous result of `norm_L2` (squaring it), to pass 
	to `dot_inc`, but this operation of repeatedly taking the square root and then squaring
	the result yields inaccuracies (e.g., when the vector contains integers)
 
	## Large incremental state

	In some cases, the incremental state must save the previous input, or output, or both.
	For example, suppose that the function is a linear mapping,
	\f$A X\f$, where \f$ A \f$ is a matrix. The from-scratch computation takes time \f$O(n^2)\f$,
	(where A is assumed a square matrix). This can be reduced to \f$O(kn)\f$, where \f$k\f$ is 
	the size of the delta vector, but at a cost of \f$O(n)\f$ space for the incremental state.
	Still, this is much better than the from-scratch time cost, if k<<n. Remember, incremental
	computations are about saving time, not necessarily space.

	In more complicated cases, even more memory must be spent.

	## Functions with more than one argument

	When functions take multiple arguments (say X1, ...,  Xn) the API looks like
	~~~~
	F(X_1, ..., X_n)
	F_with_inc(S, X_1, ... , X_n)  // if required
	F_inc(S, DX1, X2, ..., X_n)   // or  F_inc(S, DX1, X1, X2, ... , Xn)
	F_inc(S, X1, DX2, ... , X_n)  // or  F_inc(S, X1, DX2, X2, ... , Xn)
	  ... all the way to
	F_inc(S, X_1, ...,  DX_n)     /// or F_inc(S, X1, X2, ... , DXn, Xn)
	~~~~
	When the DX_i type is distinct from the X_i, function overloading can resolve the function to apply.
	When for some reason this is not possible or desirable, the API will add numeric suffix to the
	function name, e.g.,
	~~~~
	F_inc_2(S, X1, DX2, X3)
	~~~~
	## Different incremental algorithms for the same function.

	When different incremental algorithms exist, the user may select the one desired (e.g., based on
	knowledge on the type of the updates). There is no change to the naming convention in is case.
	Instead, function overloading on the type of S, and/or DX, will resolve the choice.

	## Function objects

	Some functions are defined as objects by overloading `operator()` of the class (esp. when the function takes
	parameters, as for example, safe zone functions). In this case, the incremental calls will be provided by 
	additional function methods, using the same conventions as above, except that instead of prefix `F_` we get
	prefix `F.`. 

	For example:
	~~~
	MyFunc F(...init...);

	F(X) // from-scratch call
	F.inc(S, DX)
	F.with_inc(S, X)
	~~~
	You get the idea!


  */

#include <map>
#include <numeric>
#include <valarray>
#include <iostream>
#include <algorithm>
#include <functional>

#include <boost/numeric/ublas/vector.hpp>
#include <boost/numeric/ublas/vector_sparse.hpp>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/moment.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/rolling_variance.hpp>

#include "binc.hh"

namespace hdv {

/*
	Vector functions based on valarray
 */

using std::valarray;

/** A real vector */
typedef valarray<double> Vec;

/** A vector of size_t, useful as an
    index to Vec
  */
typedef valarray<size_t> Index;

/** A vector of bool, useful as an
    index to Vec
  */
typedef valarray<bool> Mask;



/**
	A delta vector describes old and new values of a vector, 
	after an update is applied to some cells.

	Assume that you have a Vec \c X, and wish to update a small
	number of its elements, described by Index \c index.
	Then, the triple of values  (index, xold, xnew) describe
	the change of the vector. 

	In particular, the semantics are as follows: for each 
	\c 0<=i<size(), the value of the original vector's element
	\c index[i] changed from \c xold[i] to \c xnew[i].

	Important invariant: \c index must be sorted!
 */
struct delta_vector
{
	Index index;		/// index of change
	Vec xold, xnew;		/// old and new values

	delta_vector() { }
	delta_vector(size_t n) : index(n), xold(n), xnew(n) {}
	delta_vector(const Index& i) : index(i), 
		xold(0.0, index.size()), xnew(0.0, index.size()) {}
	delta_vector(Index&& i) : 
		index(i), xold(0.0, index.size()), xnew(0.0, index.size()) {}

	delta_vector(const delta_vector&)=default;
	delta_vector(delta_vector&&)=default;
	delta_vector& operator=(const delta_vector&)=default;
	delta_vector& operator=(delta_vector&&)=default;

	void resize(size_t n) {
		if(index.size()!=n) {
			index.resize(n);
			xold.resize(n);
			xnew.resize(n);
		}
	}

	size_t size() const { return index.size(); }

	inline void swap(delta_vector& other) {
		index.swap(other.index);
		xold.swap(other.xold);
		xnew.swap(other.xnew);
	}

	// Some operations to allow convenient calculation of deltas of expressions,
	// needed when we call an incremental function on an expression of the
	// original vector
	inline delta_vector& operator+=(const Vec& a) { xold += a[index]; xnew += a[index]; return *this; }
	inline delta_vector& operator-=(const Vec& a) { xold -= a[index]; xnew -= a[index]; return *this; }
	inline delta_vector& operator*=(const Vec& a) { xold *= a[index]; xnew *= a[index]; return *this; }
	inline delta_vector& operator/=(const Vec& a) { xold /= a[index]; xnew /= a[index]; return *this; }

	inline delta_vector& operator+=(double a) { xold += a; xnew += a; return *this; }
	inline delta_vector& operator-=(double a) { xold -= a; xnew -= a; return *this; }
	inline delta_vector& operator*=(double a) { xold *= a; xnew *= a; return *this; }
	inline delta_vector& operator/=(double a) { xold /= a; xnew /= a; return *this; }

	inline delta_vector& negate() { xold = -xold; xnew = -xnew; return *this; }

	/**
		\brief Apply this delta to a vector.

		That is, a +=  xnew-xold
	  */
	inline void apply_delta(Vec& a) {  a[index] += xnew - xold; }

	/**
	 	Reset to a new base vector plus the delta.
		
		This call makes \c xold equal to \c a[index] and changes \c xnew so
		that \c xnew-xold remains unchanged.
	 */
	inline void rebase(const Vec& a) { 	xnew -= xold; xnew += a[index]; xold = a[index]; }

	/**
		Reset to a new base of the 0 vector.
	  */
	inline void rebase() { 	xnew -= xold; xold = 0.0; }


	/**
		\brief Return a delta vector containing only masked coordinates.

		The mask size must be equal to \c size(). 

		@param mask the mask denoting the elements to be selected
		@return a new \c delta_vector with a number of elements equal to the number of \c true elements
		  of mask
	  */
	delta_vector operator[](const Mask& mask) const;

	/**
		Sort the entries of this \c delta_vector
	  */
	void sort();

 };



template <typename Func>
delta_vector combine_deltas(const delta_vector& v1, const delta_vector& v2, Func func)
{
	//
	// This code makes the case that v1.index and v2.index are sorted
	//

	// make a pass over the indices to count the combined indices
	size_t j=0;
	size_t i1=0, i2=0;
	while(i1 < v1.size() && i2 < v2.size()) {
		if(v1.index[i1] < v2.index[i2]) 
			i1++;
		else if(v1.index[i1] > v2.index[i2]) 
			i2++;
		else {
			i1++; i2++;
		}

		j ++;
	}
	j += v1.size() - i1 + v2.size()-i2;

	// now apply the function on the result
	delta_vector res(j);

	j=i1=i2=0;

	while(i1 < v1.size() && i2 < v2.size()) {
		if(v1.index[i1] < v2.index[i2]) {
			res.index[j] = v1.index[i1];
			res.xold[j] = func(v1.xold[i1], 0.0);
			res.xnew[j] = func(v1.xnew[i1], 0.0);
			i1++;
		}
		else if(v1.index[i1] > v2.index[i2]) {
			res.index[j] = v2.index[i2];
			res.xold[j] = func(0.0, v2.xold[i2]);
			res.xnew[j] = func(0.0, v2.xnew[i2]);
			i2++;
		}
		else {
			res.index[j] = v2.index[i2];
			res.xold[j] = func(v1.xold[i1], v2.xold[i2]);
			res.xnew[j] = func(v1.xnew[i1], v2.xnew[i2]);
			i1++; i2++;
		}
		j ++;
	}

	while(i1 < v1.size()) {
		res.index[j] = v1.index[i1];
		res.xold[j] = func(v1.xold[i1], 0.0);
		res.xnew[j] = func(v1.xnew[i1], 0.0);
		i1++;
		j++;		
	}	

	while(i2 < v2.size()) {
		res.index[j] = v2.index[i2];
		res.xold[j] = func(0.0, v2.xold[i2]);
		res.xnew[j] = func(0.0, v2.xnew[i2]);
		i2++;
		j++;		
	}	

	return res;
}



inline delta_vector operator+(const delta_vector& v1, const delta_vector& v2)
{  return combine_deltas(v1, v2, std::plus<double>() );  }

inline delta_vector operator-(const delta_vector& v1, const delta_vector& v2)
{  return combine_deltas(v1, v2, std::minus<double>() );  }

inline delta_vector operator*(const delta_vector& v1, const delta_vector& v2)
{  return combine_deltas(v1, v2, std::multiplies<double>() );  }

inline delta_vector operator/(const delta_vector& v1, const delta_vector& v2)
{  return combine_deltas(v1, v2, std::divides<double>() );  }




/*************************************************
 *
 *  Dot product
 *
 *************************************************/



/**
	Return the dot product of two valarray objects.

	This method calls internally the std::inner_product
	algorithm of STL

	@param a the first vector
	@param b the second vector
	@return the dot product of the two vectors

	@cref dot
  */
template <typename T>
inline auto dot(const valarray<T>& a, const valarray<T>& b)
{
	return std::inner_product(begin(a),end(a),begin(b), (T)0);
}


/**
	Return \f$ x^2 \f$, the dot product of a vector with itself.

	This is just a shortcut for \c dot(v,v).
  */
template <typename T>
inline auto dot(const valarray<T>& v)
{
	return dot(v,v);
}


/**
	Incremental dot product.
 */
inline double dot_inc(double& olddot, const Vec& x, const delta_vector& dy)
{
	olddot += (x[dy.index]*(dy.xnew-dy.xold)).sum();
	return olddot;
}

/**
	Incremental dot product.
 */
inline double dot_inc(double& olddot, const delta_vector& dx, const Vec& y)
{
	olddot += ((dx.xnew-dx.xold)*y[dx.index]).sum();
	return olddot;
}

/**
	Incremental dot product.
 */
inline double dot_inc(double& olddot, const delta_vector& dx)
{
	olddot += dot(dx.xnew) - dot(dx.xold);
	return olddot;
}




/*************************************************
 *
 *  Order statistics (quantiles)
 *
 *************************************************/



/**
	Return the k-th order statistic from an array.
  */
template <typename T>
inline T order_select(int k, int n, T* ptr) {
        std::nth_element(ptr, ptr+k, ptr+n);
        return ptr[k];
}


/**
	Return the k-th order statistic of a Vec.
  */
double order_select(size_t k, Vec v);



/**
   Return the median of a Vec.
  */
double median(Vec v);


// Norms 


/**
	Return the L1 norm of a vector
  */
double norm_L1(const Vec& v);

double norm_L1_inc(double& S, const delta_vector& v);



/**
	Return the L2 (Euclidean) norm of a vector
  */
double norm_L2(const Vec& v);

double norm_L2_with_inc(double& S, const Vec& v);

double norm_L2_inc(double& S, const delta_vector& dv);



/**
	Return the L_inf (Checbyshev) norm of a vector
  */
double norm_Linf(const Vec& v);


//
// Some useful functions
//

template <typename T>
inline T sq(const T& x) { return x*x; }

template <typename T>
inline T cb(const T& x) { return x*x*x; }

template <typename T> 
inline int sgn(T val) { return (T(0) < val) - (val < T(0)); }


/**
	Return the relative error between an exact
	value and an estimate
  */
inline double relative_error(double exact, double estimate)
{
	return (exact==0.0)?
	 	(estimate==0.0 ? 0.0 : estimate)
	 	: fabs((exact-estimate)/exact);
}



/**
	Return a random vector with elements drawn uniformly from \f$[a,b]\f$.
  */
Vec uniform_random_vector(size_t n, double a, double b);



/**
	A sparse vector-like object.

	It is used to hold a frequency count over a stream of observations. It cannot
	be properly called a vector, since its "dimension" is unknown. It much more
	resembles a "materialized function".

	The `Domain` type can be any type usable as a key to `std::map`. The range
	should be a numeric type.
  */
template <typename Domain, typename Range=long int, typename Compare=std::less<Domain> >
class frequency_vector : public std::map<Domain, Range, Compare>
{
	static_assert(std::is_arithmetic<Range>::value, 
		"non-arithmetic range in histogram is not allowed");
public:
	typedef Domain domain_type;
	typedef Range range_type;
	typedef std::map<Domain, Range> container;
	typedef typename container::iterator iterator;
	typedef typename container::const_iterator const_iterator;


	using std::map<Domain,Range>::begin;
	using std::map<Domain,Range>::end;

	inline Range& get_counter(const Domain& key) {
		auto loc = this->find(key);
		if(loc==this->end()) {
			return this->insert(std::make_pair(key, (Range)0)).first->second;
		}
		else {
			return loc->second;
		}		
	}

	// overload the std::map [] operators so as to avoid surprises!
	inline Range& operator[](const Domain& key) {
		return get_counter(key);
	}
	inline Range operator[](const Domain& key) const {
		auto loc = this->find(key);
		if(loc==this->end())
			return ((Range)0);
		else
			return loc.second;
	}

	/**
		Pack a frequency vector by deleting 0 entries.

	  	This method takes linear time.
	  */
	void pack() {
		// do a "delayed erase"
		iterator prev = end();
		for(auto iter=begin(); iter!=end(); iter++) {
			if(prev!=end()) {
				erase(prev);
			}
			if(iter.second != ((Range)0)) {
				prev = end();
			} else {
				prev = end();
			}
		}
		return *this;
	}

	/** Convenience method that calls `pack()` and returns *this */
	inline auto packed() { pack(); return *this; }

	/**
		Return true if there is mapping for a key

		Note that a mapping may exist and be `((Range)0)` if the vector is unpacked.
	  */
	inline bool mapping_exists(const Domain& key) const { return find(key)!=end(); }

	/**
		Declare this object as a function : `Domain` -> `Range`
	*/
	inline Range operator()(const Domain& x) const { return (*this)[x]; }


	/**
		Call f( x ) for x in keys
	  */
	template <typename Func>
	void foreach_key(const Func& f) const { for(auto it : (*this)) f(it->first); }
	/**
		Call f( self(x) ) for x in keys
	  */
	template <typename Func>
	void foreach_value(const Func& f) { for(auto it : (*this)) f(it->value); }

	/**
		Call f( x, self(x) ) for x in keys
	  */
	template <typename Func>
	void foreach_point(const Func& f) { for(auto it : (*this)) f(it->first, it->second); }

	/*
		Note: we don't call these `assign` since this method will be offered by
		std::map in C++17
	*/


	template <typename Func>
	auto pointwise_map(const Func& f) { 
		for(auto it : (*this))  it->second = f(it->second); 
		return *this;
	}
	template <typename Func>
	auto pointwise_map_point(const Func& f) { 
		for(auto it : (*this))  it->second = f(it->first, it->second); 
		return *this;
	}

	template <typename Func, typename Range2>
	auto pointwise_map(const Func& f, const frequency_vector<Domain,Range2,Compare>& other) {
		// first apply on my own domain
		for(auto it : (*this)) {
			it->second = f(it->second, other(it->first) ); 
		}
		// add the domain of the other vector
		for(auto it : other) {
			if(! mapping_exists(it->first)) 
				get_counter(it->first) = f(((Range)0), it->second);
		}
		return *this;
	}


	template <typename Func, typename Range2>
	auto pointwise_map_point(const Func& f, const frequency_vector<Domain,Range2,Compare>& other) {
		// first apply on my own domain
		for(auto it : (*this)) {
			it->second = f(it->first, it->second, other(it->first) ); 
		}
		// add the domain of the other vector
		for(auto it : other) {
			if(! mapping_exists(it->first)) 
				get_counter(it->first) = f(it->first, ((Range)0), it->second);
		}
		return *this;
	}

};



template <typename Domain, typename Range1, typename Range2, typename Compare>
inline auto inner_product(
	const frequency_vector<Domain, Range1, Compare>& v1,
	const frequency_vector<Domain, Range2, Compare>& v2
 )
{
	auto less = v1.key_comp();
	auto i1 = v1.begin();
	auto e1 = v1.end();
	auto i2 = v2.begin();
	auto e2 = v2.end();
	typedef decltype( ((Range1)0) * ((Range2)0) ) result_type;
	result_type res = 0;

	while(true) {
		if(i1==e1 || i2==e2) 
			return res;
		else if(less(i1->first, i2->first))
			++i1;
		else if(less(i2->first, i1->first))
			++i2;
		else {
			res += i1->second * i2->second;
			++i1; ++i2;
		}
	}
}


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

	/**
		Construct an observer with the given parameter for
		rolling window
	  */
	estimate_error_observer(size_t window);

	/**
		Add an error observation
	  */
	void observe(double exact, double est);
};



} // end namespace hdv



/**
	Just a printer for vectors
  */
template <typename T>
inline std::ostream& operator<<(std::ostream& s, const std::valarray<T>& a)
{
	s << "[";
	const char* sep="";
	for(auto x : a) {
		s << sep << x;
		sep = " ";
	}
	return s << "]";
}



#endif