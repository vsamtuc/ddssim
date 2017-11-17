
#include "binc.hh"
#include "safezone.hh"

using namespace gm;
using namespace dds;
using namespace binc;



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
	const_Vec_sketch_view x = make_sketch_view(proj, X);
	Vec z = sqrt_T - sqrt(dot_estvec(x));
	return Median(z);
}

double selfjoin_agms_safezone_upper_bound::with_inc(incremental_state& incstate, const Vec& X) 
{
	const_Vec_sketch_view x = make_sketch_view(proj, X);
	incstate = dot_estvec(x);
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
	const_Vec_sketch_view x = make_sketch_view(Ehat.proj,X);
	Vec z = dot_estvec(x,Ehat) - sqrt_T ;
	return Median(z);
}

double selfjoin_agms_safezone_lower_bound::with_inc(incremental_state& incstate, const Vec& X)
{
	if(sqrt_T==0.0) return INFINITY;
	const_Vec_sketch_view x = make_sketch_view(Ehat.proj,X);
	incstate = dot_estvec(x,Ehat);
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
//  twoway_join_agms_safezone_upper_bound
//  twoway_join_agms_safezone_lower_bound
//  twoway_join_agms_safezone
//
/////////////////////////////////////////////////////////






/////////////////////////////////////////////////////////
//
//  Query objects
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
	safe_zone = std::move(selfjoin_agms_safezone(Eview(), Tlow, Thigh, true)); 

	zeta_E = safe_zone(E);
}


double selfjoin_query_state::query_func(const Vec& x)
{
	return dot_est(make_sketch_view(proj, E));
}



/////////////////////////////////////////////////////////
//
//  hyperbola distance routines
//
/////////////////////////////////////////////////////////

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

#define Y(x) sqrt(sq(x)+T)
#define g(x) (2. - p/(x) - q/Y(x))

	// find upper and lower bounds for root
	double x1 = copysign(max(fabs(p),q), p);
	assert(g(x1)>0);
	double x0 = 0;
	double xm = (x0+x1)/2;

	size_t loops = 100;
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
	//binc::print("loop=",loops," \\xi=",xm, " x0=",x0, " x1=",x1);

#undef Y
#undef g
	return xm;
}


/////////////////////////////////////////////////////////
//
//  bilinear 2d safe zone
//
/////////////////////////////////////////////////////////

bilinear_2d_safe_zone::bilinear_2d_safe_zone() 
: safezone_func(false) {}

bilinear_2d_safe_zone::bilinear_2d_safe_zone(double xi, double psi, double _T)
	: T(_T), xihat(sgn(xi)), u(0.0), v(0.0)
{
	if(sq(xi)-sq(psi) < T)
		throw std::invalid_argument("the reference point is non-admissible");

	// cache the conic safe zone, if applicable
	if(T<0) {
		u = hyperbola_nearest_neighbor(xi, fabs(psi), -T);
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

		double v = hyperbola_nearest_neighbor(y, x_xihat, T);
		double u = sqrt(sq(v)+T);

		return sgn_delta*sqrt(sq(x_xihat - u) + sq(y - v));
	} 
	else {
		return u*x - v*fabs(y) - T;
	} 
}

