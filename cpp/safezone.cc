
#include "binc.hh"
#include "safezone.hh"

#include <boost/math/tools/roots.hpp>

using namespace gm;
using namespace dds;
using namespace binc;


/////////////////////////////////////////////////////////
//
//  query_state
//
/////////////////////////////////////////////////////////


query_state::query_state(size_t _D)
	: E(_D)
{ 
	E = 0.0;
}


/////////////////////////////////////////////////////////
//
//  quorum_safezone
//
/////////////////////////////////////////////////////////

quorum_safezone::quorum_safezone() 
: safezone_func(false) { }

quorum_safezone::quorum_safezone(const Vec& zE, size_t _k, bool _eik) 
{
	prepare(zE, _k);
	set_eikonal(_eik);
}

void quorum_safezone::prepare(const Vec& zE, size_t _k) 
{
	n = zE.size();
	k = _k;

	// count legal inputs
	size_t Legal[n];
	size_t pos = 0;
	for(size_t i=0; i<n; i++)
		if(zE[i]>0) Legal[pos++] = i;

	// create the index and the other matrices
	Index Ltmp(Legal, pos);
	Ltmp.swap(L);
	zetaE.resize(L.size());
	zetaE = zE[L];

	assert(1<=k && k<=n);
	if(L.size()<k)
		throw std::length_error("The reference vector is non-admissible");
}


/*
	Helper to compute sum(zEzX[I])/sqrt(sum(zEzE[I]^2))
 */
inline static double zeta_I(size_t *I, size_t m, const Vec& zEzX, const Vec& zEzE)
{
	double num = 0.0;
	double denom = 0.0;
	for(size_t i=0; i<m; i++) {
		num += zEzX[I[i]];
		denom += zEzE[I[i]];
	}
	return num/sqrt(denom);
}

/*
	Helper to iterate over all strictly increasing m-long
	sequences over [0:l)
  */
inline static bool next_I(size_t *I, size_t m, size_t l) 
{
	for(size_t i=1; i<=m; i++) {
		if(I[m-i] < l-i) {
			I[m-i]++;
			for(size_t j=1;j<i;j++) 
				I[m-i+j] = I[m-i]+j;
			return true;
		}
	}
	return false;
}


double quorum_safezone::func_eikonal(const Vec& zX) 
{
	// precompute  zeta_i(E)*zeta_i(X) for all i\in L
	Vec zEzX = zetaE;
	zEzX *= zX[L];

	// precompute zeta_i(E)^2
	Vec zE2 = zetaE*zetaE;

	size_t l = L.size();
	size_t m = L.size()-k+1;

	//
	// Each clause corresponds to an m-size subset I of [0:l).
	// The goal is to find the subset I where
	//   (\sum_i\in I  zXzE[i])  /  sqrt(\sum_i zE2) = zeta_I(I)
	// In lieu of a smart heuristic, this is now done exhaustively.
	//

	size_t I[m]; // holds the indices to the current m-set
	std::iota(I, I+m, 0);  // initialized to (0,...,0)

	// iteration is done with the help of next_I, which increments I

	double zinf = zeta_I(I, m, zEzX, zE2);
	while(next_I(I, m, l)) {
		double zI = zeta_I(I, m, zEzX, zE2);
		zinf = std::min(zI, zinf);
	}

	return zinf;
}



double quorum_safezone::func_non_eikonal(const Vec& zX) 
{
	Vec zEzX = zetaE;
	zEzX *= zX[L];
	std::nth_element(begin(zEzX), begin(zEzX)+(L.size()-k), end(zEzX));
	return std::accumulate(begin(zEzX), begin(zEzX)+(L.size()-k+1), 0.0);
}


/////////////////////////////////////////////////////////
//
//  selfjoin_agms_safezone_upper_bound
//  selfjoin_agms_safezone_lower_bound
//  selfjoin_agms_safezone
//
/////////////////////////////////////////////////////////



double selfjoin_agms_safezone_upper_bound::operator()(const Vec& X) 
{
	Vec z = sqrt_T - sqrt(dot_estvec(proj(X)));
	return Median(z);
}

double selfjoin_agms_safezone_upper_bound::with_inc(incremental_state& incstate, const Vec& X) 
{
	incstate = dot_estvec(proj(X));
	Vec z = sqrt_T - sqrt(incstate);
	return Median(z);
}


double selfjoin_agms_safezone_upper_bound::inc(incremental_state& incstate, const delta_vector& DX) 
{
	Vec z = sqrt_T - sqrt(dot_estvec_inc(incstate, DX));
	return Median(z);
}



double selfjoin_agms_safezone_lower_bound::operator()(const Vec& X) 
{
	if(sqrt_T==0.0) return INFINITY;
	Vec z = dot_estvec(Ehat.proj(X),Ehat) - sqrt_T ;
	return Median(z);
}

double selfjoin_agms_safezone_lower_bound::with_inc(incremental_state& incstate, const Vec& X)
{
	if(sqrt_T==0.0) return INFINITY;
	incstate = dot_estvec(Ehat.proj(X),Ehat);
	Vec z = incstate - sqrt_T;
	return Median(z);
}


double selfjoin_agms_safezone_lower_bound::inc(incremental_state& incstate, const delta_vector& DX)
{
	if(sqrt_T==0.0) return INFINITY;
	Vec z = dot_estvec_inc(incstate, DX, Ehat) - sqrt_T ;
	return Median(z);
}



// from-scratch computation
double selfjoin_agms_safezone::operator()(const Vec& X) 
{
	return min(lower_bound(X), upper_bound(X));
}
double selfjoin_agms_safezone::operator()(const Vec& X, double& zeta_l, double& zeta_u) 
{
	zeta_l = lower_bound(X);
	zeta_u = upper_bound(X);
	return min(zeta_l, zeta_u);
}


double selfjoin_agms_safezone::with_inc(incremental_state& incstate, const Vec& X)
{
	return min(lower_bound.with_inc(incstate.lower, X), 
		upper_bound.with_inc(incstate.upper, X));
}
double selfjoin_agms_safezone::with_inc(incremental_state& incstate, const Vec& X,
					double& zeta_l, double& zeta_u)
{
	return min( (zeta_l = lower_bound.with_inc(incstate.lower, X)), 
		    (zeta_u = upper_bound.with_inc(incstate.upper, X)) );
}

double selfjoin_agms_safezone::inc(incremental_state& incstate, const delta_vector& DX) {
	return min(lower_bound.inc(incstate.lower, DX), 
		upper_bound.inc(incstate.upper, DX));
}
double selfjoin_agms_safezone::inc(incremental_state& incstate, const delta_vector& DX,
				   double& zeta_l, double& zeta_u) {
	return min( (zeta_l = lower_bound.inc(incstate.lower, DX)),
		    (zeta_u = upper_bound.inc(incstate.upper, DX)) );
}



/////////////////////////////////////////////////////////
//
//  selfjoin_query_state
//
/////////////////////////////////////////////////////////


selfjoin_query_state::selfjoin_query_state(double _beta, projection _proj)
	: query_state(_proj.size()),
	  beta(_beta), proj(_proj), 
	  epsilon(_proj.epsilon())
{
	assert(norm_Linf(E)==0.0);
	if( epsilon >= beta )
		throw std::invalid_argument("total error is less than sketch error");
	compute();
	assert(fabs(zeta_E-sqrt( (_proj.depth()+1)/2))<1E-15);
}


void selfjoin_query_state::update_estimate(const Vec& newE)
{
	// compute the admissible region
	E += newE;
	compute();
}

void selfjoin_query_state::compute()
{
	Qest = query_func(E);

	if(Qest>0) {
		Tlow = (1+epsilon)*Qest/(1.0+beta);
		Thigh = (1-epsilon)*Qest/(1.0-beta);
	}
	else {
		Tlow = 0.0; Thigh=1.0;
	}
	safe_zone = std::move(selfjoin_agms_safezone(proj(E), Tlow, Thigh, true)); 

	zeta_E = safe_zone(E);
}


double selfjoin_query_state::query_func(const Vec& x)
{
	return dot_est(proj(E));
}


void* selfjoin_query_state::alloc_incstate() 
{
	return new selfjoin_agms_safezone::incremental_state;
}

void selfjoin_query_state::free_incstate(void* ptr) 
{
	delete static_cast<selfjoin_agms_safezone::incremental_state*>(ptr);
}

double selfjoin_query_state::compute_zeta(void* inc, const delta_vector& dU, const Vec& U)
{
	auto incstate = static_cast<selfjoin_agms_safezone::incremental_state*>(inc);
	delta_vector DU = dU;
	DU += E;
	return safe_zone.inc(*incstate, DU);
}

double selfjoin_query_state::compute_zeta(void* inc, const Vec& U)
{
	auto incstate = static_cast<selfjoin_agms_safezone::incremental_state*>(inc);
	Vec X = U+E;
	return safe_zone.with_inc(*incstate, X);
}




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

	double x0 = copysign(fabs(p)/(2.5+fabs(q)/sqrt(T)), p);
	double g0 = g(x0);
	double x1 = copysign(max(fabs(p),q), p);
	double g1 = g(x1);
	assert(g0<0);
	assert(g1>0);

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
: safezone_func(false) {}

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



/////////////////////////////////////////////////////////
//
//  twoway_join_agms_safezone
//
/////////////////////////////////////////////////////////



twoway_join_agms_safezone::twoway_join_agms_safezone() 
	: safezone_func(false) 
{ }



twoway_join_agms_safezone::bound::bound() { }

twoway_join_agms_safezone::bound::bound(const projection& _proj, double _T, bool eikonal) 
: proj(_proj), T(_T), 
  hat(_proj.size())
{ 
	zeta_2d.reserve(proj.depth());
	Median.set_eikonal(eikonal);
}

void twoway_join_agms_safezone::bound::setup(const Vec& norm_xi, const Vec& norm_psi) {
	Vec zeta_E(proj.depth()); // vector to prepare the median SZ
	Vec tmp(proj.width());

	for(size_t i=0; i<proj.depth(); i++) {
		// create the bilinear 2d safe zones 
		zeta_2d.emplace_back(norm_xi[i], norm_psi[i], 4.0*T);

		// compute the zeta_E vector for the median
		zeta_E[i] = zeta_2d[i](norm_xi[i], norm_psi[i])*sqrt(0.5);

		// Normalize the hat vector
		slice I(i*proj.width(), proj.width(), 1);
		if(norm_xi[i]>0.0) {
			tmp = norm_xi[i];
			hat[I] /= tmp;
		} 
		else {
			tmp = 0.0;
			hat[I] = tmp;
		}
	}

	Median.prepare(zeta_E, (proj.depth()+1)/2);
}


double twoway_join_agms_safezone::bound::zeta(incremental_state& inc, const Vec& x, const Vec& y)
{
	// set inc
    inc.x2 = dot_estvec(proj(x), proj(hat));
    inc.y2 = dot_estvec(proj(y));
    //print(this, "from scratch: x2=",inc.x2,"y2=",inc.y2);
    return zeta(inc.x2, inc.y2);
}

double twoway_join_agms_safezone::bound::zeta(incremental_state& inc, const delta_vector& dx, const delta_vector& dy)
{
	// update inc
	Vec& x2 = dot_estvec_inc(inc.x2, dx, proj(hat));
	Vec& y2 = dot_estvec_inc(inc.y2, dy);
    //print(this, "incremental: x2=",x2,"y2=",y2);
	return zeta(x2, y2);
}

double twoway_join_agms_safezone::bound::zeta(const Vec& x2, const Vec& y2)
{
    Vec zeta_X(proj.depth());

    for(size_t i=0; i<proj.depth(); i++)
    	zeta_X[i] = zeta_2d[i]( x2[i], sqrt(y2[i]) )*sqrt(0.5);
    return Median(zeta_X);			
}

twoway_join_agms_safezone::twoway_join_agms_safezone(const Vec& E, const projection& proj, 
							double Tlow, double Thigh, bool eikonal)
	: 	D(proj.size()), 
		lower(proj, Tlow, eikonal), 
		upper(proj, -Thigh, eikonal)
{
	assert(E.size() == 2*proj.size());
	assert(Tlow < Thigh);

	// Polarize the reference vector
	slice s1(0, D, 1);
	slice s2(D, D, 1);

	lower.hat = E[s1] + E[s2];
	upper.hat = E[s1] - E[s2];

	// Initialize the upper and lower bound safe zone data
	Vec norm_lower =  sqrt(dot_estvec(proj(lower.hat)));
	Vec norm_upper = sqrt(dot_estvec(proj(upper.hat)));

	lower.setup(norm_lower, norm_upper);
	upper.setup(norm_upper, norm_lower);
}


double twoway_join_agms_safezone::with_inc(incremental_state& inc, const Vec& U)
{
	assert(U.size() == 2*D);

	// Polarize
	slice s1(0, D, 1);
	slice s2(D, D, 1);

    inc.x = U[s1] + U[s2];
    inc.y = U[s1] - U[s2];

    // Compute zeta of lower bound
    double zeta_lower = lower.zeta(inc.lower, inc.x, inc.y);

    // Compute zeta of upper bound
    double zeta_upper = upper.zeta(inc.upper, inc.y, inc.x);

    //binc::print("fromscratch zlower=",zeta_lower,"zupper",zeta_upper);
    return min(zeta_lower, zeta_upper);
}


// from-scratch computation, just use with_inc, with a throw-away incremental state
double twoway_join_agms_safezone::operator()(const Vec& X)
{
	incremental_state inc;
	return with_inc(inc, X);
}


double twoway_join_agms_safezone::inc(incremental_state& incstate, const delta_vector& DX)
{
	// Polarize the delta
	delta_vector DX1 = DX[DX.index < D];
	delta_vector DX2 = DX[DX.index >= D];
	DX2.index -= D;

	delta_vector dx = DX1+DX2;
	delta_vector dy = DX1-DX2;

	dx.rebase(incstate.x);
	dy.rebase(incstate.y);

	// update the polarization incstate
	incstate.x[dx.index] = dx.xnew;
	incstate.y[dy.index] = dy.xnew;

    // Compute zeta of lower bound
    double zeta_lower = lower.zeta(incstate.lower, dx, dy);

    // Compute zeta of upper bound
    double zeta_upper = upper.zeta(incstate.upper, dy, dx);
    //binc::print("incremental zlower=",zeta_lower,"zupper",zeta_upper);
    return min(zeta_lower, zeta_upper);
}


twoway_join_query_state::twoway_join_query_state(double _beta, projection _proj)
    : query_state(_proj.size()),
      proj(_proj), beta(_beta),
      epsilon(_proj.epsilon())
{
	if( epsilon >= beta )
		throw std::invalid_argument("total error is less than sketch error");
	compute();	
}

void twoway_join_query_state::update_estimate(const Vec& newE)
{
    // compute the admissible region
    E += newE;
    compute();
}

double twoway_join_query_state::query_func(const Vec& x)
{
	assert(x.size() == E.size());
	auto x0 = std::begin(x);
	auto x1 = x0 + E.size()/2;
	auto x2 = x1 + E.size()/2;
	return dot_est(proj(x0,x1), proj(x1,x2));
}

void twoway_join_query_state::compute()
{
    Qest = query_func(E);

    if(Qest!=0.0) {
            Tlow =  Qest - (beta-epsilon)*fabs(Qest)/(1.0+beta);
            Thigh = Qest + (beta-epsilon)*fabs(Qest)/(1.0-beta);
    }
    else {
            Tlow = 0.0; Thigh=1.0;
    }
    safe_zone = std::move(twoway_join_agms_safezone(E, proj, Tlow, Thigh, true));
    zeta_E = safe_zone(E);
}


void* twoway_join_query_state::alloc_incstate() 
{
	return new twoway_join_agms_safezone::incremental_state;
}

void twoway_join_query_state::free_incstate(void* ptr)
{
	auto incstate = static_cast<twoway_join_agms_safezone::incremental_state*>(ptr);
	delete incstate;
}

double twoway_join_query_state::compute_zeta(void* inc, const delta_vector& dU, const Vec& U)
{
	auto incstate = static_cast<twoway_join_agms_safezone::incremental_state*>(inc);
	delta_vector DU = dU;
	DU += E;
	return safe_zone.inc(*incstate, DU);
}

double twoway_join_query_state::compute_zeta(void* inc, const Vec& U) 
{
	auto incstate = static_cast<twoway_join_agms_safezone::incremental_state*>(inc);
	Vec X = U+E;
	return safe_zone.with_inc(*incstate, X);
}


