
#include <random>
#include <algorithm>
#include <vector>
#include <cassert>

#include "hdv.hh"

using namespace hdv;


delta_vector delta_vector::operator[](const Mask& m) const 
{
	assert(m.size() == index.size());
	size_t retsize = std::count(begin(m), end(m), true);
	delta_vector ret( retsize );
	size_t j=0;
	for(size_t i=0;i<index.size();i++) {
		if(m[i]) {
			ret.index[j] = index[i];
			ret.xold[j] = xold[i];
			ret.xnew[j] = xnew[i];
			j++;
		}
	}
	assert(j==retsize);
	return ret;		
}


void delta_vector::sort()
{
	// Compute a permutation of the elements of index
	std::vector<size_t> P(size());
	std::iota(P.begin(), P.end(), 0);

	std::sort(P.begin(), P.end(), [&](size_t i, size_t j) { return index[i]<index[j]; });

	// Use a temp array for the permutation of xold, xnew
	Vec temp = xold;
	for(size_t i=0; i<P.size(); i++) 
		xold[i] = temp[P[i]];
	temp = xnew;
	for(size_t i=0; i<P.size(); i++) 
		xnew[i] = temp[P[i]];
	Index tmp = index;
	for(size_t i=0; i<P.size(); i++) 
		index[i] = tmp[P[i]];
}



double hdv::order_select(size_t k, Vec v)
{
	if(k>=v.size()) throw std::length_error("order exceeds vector length");
	std::nth_element(begin(v), begin(v)+k, end(v));
	return v[k];
}


double hdv::median(Vec v)
{
	const auto n = v.size();
	if(n==0) throw std::length_error("median called on 0-size vector");
	if(n & 1) {
		// odd order
		std::nth_element(begin(v), begin(v)+n/2, end(v));
		return v[n/2];
	} else {
		// even order
		auto k = n/2;
		std::nth_element(begin(v), begin(v)+k, end(v));
		double m = v[k];
		k--;
		std::nth_element(begin(v), begin(v)+k, end(v));
		return (m+v[k])*0.5;
	}
}


double hdv::norm_L1(const Vec& v)
{
	double sum=0.0;
	for(double x : v)
		sum += fabs(x);
	return sum;
}

double hdv::norm_L1_inc(double& S, const delta_vector& dv)
{
	S += norm_L1(dv.xnew) - norm_L1(dv.xold);
	return S;
}


double hdv::norm_L2(const Vec& v)
{
	double sum=0.0;
	for(double x : v) sum+=x*x;
	return sqrt(sum);
}


double hdv::norm_L2_with_inc(double& S, const Vec& v)
{
	S=0.0;
	for(double x : v) S+=x*x;
	return sqrt(S);
}

double hdv::norm_L2_inc(double& S, const delta_vector& dv)
{
	return sqrt(dot_inc(S, dv));
}



double hdv::norm_Linf(const Vec& v)
{
	double l = 0.0;
	for(double x : v) {
		double xabs = fabs(x);
		if(l<xabs) l=xabs;
	}
	return l;
}


static std::mt19937 RNG(24534623);

Vec hdv::uniform_random_vector(size_t n, double a, double b)
{	
	using namespace std;
	Vec v(n);
	uniform_real_distribution<> U(a,b);
	generate(begin(v), end(v), [&]() { return U(RNG); });
	return v;
};


