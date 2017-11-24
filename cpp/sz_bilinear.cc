#include <cassert>
#include <algorithm>
#include <boost/math/tools/roots.hpp>
#include "sz_bilinear.hh"

using namespace std;
using namespace gm;


/////////////////////////////////////////////////////////
//
//  hyperbola distance routines
//
/////////////////////////////////////////////////////////


#define USE_BISECTION 0

#if USE_BISECTION==1
static double __bisection(double p, double q, double T, double epsilon)
{
	/*  Precondition: T>0  and  p,q != 0.0 
        Bisection up to 50 iterations and 10^-13 precision (by default)
	*/
#define Y(x) sqrt(sq(x)+T)
#define g(x) (2. - p/(x) - q/Y(x))

	// find upper and lower bounds for root
	double x0 = copysign(fabs(p)/(2.5+fabs(q)/sqrt(T)), p);
	assert(g(x0)<0);
	double x1 = copysign(max(fabs(p),q), p);
	assert(g(x1)>0);
	double xm = (x0+x1)/2;

	size_t loops = 50;
	while( fabs((x1-x0)/xm) >= epsilon) {
		if((--loops) == 0) break;
		double gx = g(xm);
		if(gx>0)
			x1 = xm;
		else if(gx<0)
			x0 = xm;
		else
			break;
		xm = (x0+x1)/2;
	}

	return xm;
#undef Y
#undef g
}

#else

static double __toms_748(double p, double q, double T, double epsilon)
{
	/*  Precondition: T>0  and  p,q != 0.0 
        Up to 50 iterations and 10^-13 precision (by default).
        This is much faster than bisection.
	*/
	using boost::math::tools::toms748_solve;
	using boost::math::tools::newton_raphson_iterate;
	using boost::math::tools::eps_tolerance;

	auto g = [p,q,T](double x) { return 2.-p/x - q/sqrt(sq(x)+T); };
	auto tolfunc = [epsilon](const double& x0, const double& x1) {
		return fabs( 2.*(x1-x0)/(x1+x0) ) < epsilon;
	};

	double x0 = copysign(fabs(p)/(2.1+fabs(q)/sqrt(T)), p);
	double g0 = g(x0);
	double x1 = copysign(0.51*(fabs(p)+max(0.0,q)), p);
	double g1 = g(x1);
	assert(g0<=0);
	assert(g1>=0);

	if(g0==0) return x0;
	if(g1==0) return x1;

	if(x0>x1) {
		swap(x0,x1);
		swap(g0,g1);
	}

	boost::uintmax_t max_iter = 50;
	auto soln = toms748_solve(g, x0, x1, g0, g1, tolfunc, max_iter);
	return 0.5*(soln.first+soln.second);	
}
#endif

double gm::hyperbola_nearest_neighbor(double p, double q, double T, double epsilon)
{
	/*
		Consider the hyperbola \( y(x) = \sqrt{x^2+T} \), where \(T \geq 0\),
		 for \( x\in [0,+\infty) \), and
		let \( (\xi, \psi) \) be a point on it.

		First, assume \( \xi >0 \). Then, the normal to the hyperbola at that point
		is the line passing through points \( (2\xi,0) \) and \( (0,2\psi) \). To see this,
		note that \( \nabla (y^2-x^2) = (-2x, 2y) \). In other words, the normal to the
		hyperbola at \( (\xi, \psi) \) has a parametric equation of 
		\[  (\xi,\psi) + t (-2\xi, 2\psi).  \]

		Now, any point \( (p,q)\) whose nearest neighbor is \( (\xi, \psi) \) must satisfy
		\[  (p,q) =  (\xi,\psi) + t (-2\xi, 2\psi),  \]
		and by eliminating \( t\) we get 
		\[   \frac{p}{\xi} + \frac{q}{\psi} = 2.  \]
		Thus, it suffices to find the root of the function
		\[  g(x) = 2 - p/x - q/y(x) \]
		(which is unique).

		This function has \( g(0) = -\infty\), \(g(\xi) = 0\) and \(g(\max(p,q))>0\). Also,
		\[  g'(x) = \frac{p}{x^2} + \frac{qx}{2y^3}.  \]

		In case \(p=0\), it is easy to see that, if \(q>2\sqrt{T}\), then the nearest point is
		\( (\sqrt{(q/2)^2 - T}, q/2)\), and for \( q\leq 2\sqrt{T} \), the answer is \( (0, \sqrt{T}) \).
	*/

	using std::max;

	if(T<0)
		throw std::invalid_argument("call to hyperbola_nearest_neighbor with T<0");

	if(T==0.0) {
		// Direct solution
		if(p<0.0) 
			return (q<=p) ? 0.0 : 0.5*(p-q);
		else
			return (q<=-p) ? 0.0 : 0.5*(p+q);
	}

	if(p==0.0) {
		if(q > 2.*sqrt(T))
			return sqrt(sq(q/2)-T);
		else
			return 0.;
	}
	if(q==0.0)
		return p/2.;

#if USE_BISECTION==1
	return __bisection(p, q, T, epsilon);
#else
	return __toms_748(p, q, T, epsilon);
#endif

}


/////////////////////////////////////////////////////////
//
//  bilinear 2d safe zone
//
/////////////////////////////////////////////////////////

bilinear_2d_safe_zone::bilinear_2d_safe_zone() 
{}

bilinear_2d_safe_zone::bilinear_2d_safe_zone(double xi, double psi, double _T)
	: 	T(_T), 
		xihat(sgn(xi)), //xihat(xi>=0.0 ? 1: -1), 
		u(0.0), v(0.0)
{
	//if(sq(xi)-sq(psi) < T)
	//	throw std::invalid_argument("the reference point is non-admissible");

	// cache the conic safe zone, if applicable
	if(T<0) {
		u = hyperbola_nearest_neighbor(xi, fabs(psi), -T, epsilon);
		v = sqrt(sq(u)-T);
		// eikonalize
		double norm_u_v = sqrt(sq(u)+sq(v));
		assert(norm_u_v > 0);
		u /= norm_u_v;
		v /= norm_u_v;
		T /= norm_u_v;
	} else if(T==0.0) {
		u = (xi>=0.0) ? 1.0/sqrt(2.0) : -1.0/sqrt(2.0);
		v = 1.0/sqrt(2.0);
	}
}

double bilinear_2d_safe_zone::operator()(double x, double y) const 
{
	if(T>0) {
		// compute the signed distance function of the set $\{ x >= \sqrt{y^2+T} \}$.
		double x_xihat = x*xihat;

		int sgn_delta = sgn( x_xihat - sqrt(sq(y)+T) );

		double v = hyperbola_nearest_neighbor(y, x_xihat, T, epsilon);
		double u = sqrt(sq(v)+T);

		return sgn_delta*sqrt(sq(x_xihat - u) + sq(y - v));
	} 
	else {
		return u*x - v*fabs(y) - T;
	} 
}


/////////////////////////////////////////////////////////
//
//  inner product safe zone
//
/////////////////////////////////////////////////////////

inner_product_safe_zone::inner_product_safe_zone(const Vec& E, bool _geq, double _T)
: geq(_geq), T(_T)
{
	assert(E.size()%2 ==0);

	slice s1(0,E.size()/2,1);
	slice s2(E.size()/2,E.size()/2,1);

	Vec xi = E[s1]+E[s2];
	Vec psi = E[s1]-E[s2];

	if(!geq) {
		xi.swap(psi);
		T = -T;
	}

	double norm_xi = norm_L2(xi);
	double norm_psi = norm_L2(psi);

	sqdiff = bilinear_2d_safe_zone(norm_xi, norm_psi, 4.*T);

	if(norm_xi>0)
		xihat = xi/norm_xi;
	else {
		if(T<0)
			xihat = Vec(0.0, E.size()/2);
		else {
			xihat = Vec(sqrt(2./E.size()), E.size()/2);
		}
	}
}

double inner_product_safe_zone::operator()(const Vec& X) const
{
	assert(X.size() == xihat.size()*2);

	slice s1(0,xihat.size(),1);
	slice s2(xihat.size(),xihat.size(),1);

	Vec x = X[s1]+X[s2];
	Vec y = X[s1]-X[s2];
	if(!geq) x.swap(y);

	double x2 = dot(x, xihat);
	double y2 = norm_L2(y);

	return sqdiff(x2, y2)*sqrt(0.5);
}

double inner_product_safe_zone::with_inc(incremental_state& inc, const Vec& X) const
{
	assert(X.size() == xihat.size()*2);

	slice s1(0,xihat.size(),1);
	slice s2(xihat.size(),xihat.size(),1);

	Vec x = X[s1]+X[s2];
	Vec y = X[s1]-X[s2];
	if(!geq) x.swap(y);

	inc.x.resize(x.size());
	inc.x = x;
	inc.y.resize(y.size());
	inc.y = y;

	inc.x2 = dot(x, xihat);
	double y2 = norm_L2_with_inc(inc.y2, y);

	return sqdiff(inc.x2, y2)*sqrt(0.5);
}


double inner_product_safe_zone::inc(incremental_state& inc, const delta_vector& dX) const
{

	delta_vector dX1 = dX[dX.index < xihat.size()];
	delta_vector dX2 = dX[dX.index >= xihat.size()];
	dX2.index -= xihat.size();

	delta_vector dx = dX1+dX2;
	delta_vector dy = dX1-dX2;

	if(!geq) {
		dx.swap(dy);
	}

	dx.rebase(inc.x);
	dy.rebase(inc.y);

	inc.x[dx.index] = dx.xnew;
	inc.y[dy.index] = dy.xnew;

	double x2 = dot_inc(inc.x2, dx, xihat);
	double y2 = norm_L2_inc(inc.y2, dy);

	return sqdiff(x2, y2)*sqrt(0.5);	
}

