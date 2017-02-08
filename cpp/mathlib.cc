
#include "mathlib.hh"

using namespace dds;


double dds::order_select(size_t k, Vec v)
{
	if(k>=v.size()) throw std::length_error("order exceeds vector length");
	std::nth_element(begin(v), begin(v)+k, end(v));
	return v[k];
}


double dds::median(Vec v)
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


double norm_L1(const Vec& v)
{
	double sum=0.0;
	for(double x : v)
		sum += fabs(x);
	return sum;
}


double norm_L2(const Vec& v)
{
	double sum=0.0;
	for(double x : v) sum+=x*x;
	return sqrt(sum);
}


double norm_Linf(const Vec& v)
{
	double l = -1.0;
	for(double x : v) {
		double xabs = fabs(x);
		if(l<xabs) l=xabs;
	}
	return l;
}


estimate_error_observer::estimate_error_observer(size_t window) 
	: tally(tag::rolling_window::window_size = window)
	{}

void estimate_error_observer::observe(double exact, double est) {
	using namespace std;
	double err = relative_error(exact,est);
	//cout << "relerr("<<exact<<","<<est<<")=" << err << endl;
	tally(err);
}
