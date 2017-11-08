
#include "safezone.hh"


using namespace dds;


/////////////////////////////////////////////////////////
//
//  quorum_safezone
//
/////////////////////////////////////////////////////////

quorum_safezone::quorum_safezone() 
: safezone_base(false) { }

quorum_safezone::quorum_safezone(const Vec& zE, size_t _k) 
{
	prepare(zE, _k);
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


double quorum_safezone::operator()(const Vec& zX) 
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



double quorum_safezone_fast::operator()(const Vec& zX) 
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


selfjoin_agms_safezone_upper_bound::selfjoin_agms_safezone_upper_bound(const sketch& E, double T)
: 	sqrt_T(sqrt(T)), Median()
{ 
	Vec dest = sqrt(dot_estvec(E));
	Median.prepare(sqrt_T - dest , (E.depth()+1)/2) ;
}


double selfjoin_agms_safezone_upper_bound::operator()(const sketch& X) 
{
	Vec z = sqrt_T - sqrt(dot_estvec(X));
	return Median(z);
}

double selfjoin_agms_safezone_upper_bound::with_inc(incremental_state& incstate, const sketch& X) 
{
	incstate = dot_estvec(X);
	Vec z = sqrt_T - sqrt(incstate);
	return Median(z);
}


double selfjoin_agms_safezone_upper_bound::inc(incremental_state& incstate, const delta_vector& DX) 
{
	Vec z = sqrt_T - sqrt(dot_estvec_inc(incstate, DX));
	return Median(z);
}


selfjoin_agms_safezone_lower_bound::selfjoin_agms_safezone_lower_bound(const sketch& E, double T)
: 	Ehat(E), sqrt_T(sqrt(T))
{ 
	assert(T>=0.0);   // T must be positive

	//
	//  If T == 0.0, the function returns +inf
	//

	if(T>0.0) {

		Vec dest = sqrt(dot_estvec(E));
		Median.prepare( dest - sqrt_T, (E.depth()+1)/2);

		//
		// Normalize E: divide each row  E_i by ||E_i||
		//
		size_t L = E.width();
		for(size_t d=0; d<E.depth(); d++) {
			if(dest[d]>0.0)  {
				// Note: this is an aliased assignment, but should
				// be ok, because it is pointwise aliased!
				Vec tmp = Ehat[slice(d*L,L,1)];
				Ehat[slice(d*L,L,1)] = tmp/dest[d];
			}
			// else, if dest[d]==0, then Ehat[slice(d)] == 0! leave it
		}

	}

}

double selfjoin_agms_safezone_lower_bound::operator()(const sketch& X) 
{
	if(sqrt_T==0.0) return INFINITY;
	Vec z = dot_estvec(X,Ehat) - sqrt_T ;
	return Median(z);
}

double selfjoin_agms_safezone_lower_bound::with_inc(incremental_state& incstate, const sketch& X)
{
	if(sqrt_T==0.0) return INFINITY;
	incstate = dot_estvec(X,Ehat);
	Vec z = incstate - sqrt_T;
	return Median(z);
}


double selfjoin_agms_safezone_lower_bound::inc(incremental_state& incstate, const delta_vector& DX)
{
	if(sqrt_T==0.0) return INFINITY;
	Vec z = dot_estvec_inc(incstate, DX, Ehat) - sqrt_T ;
	return Median(z);
}


selfjoin_agms_safezone::selfjoin_agms_safezone(const sketch& E, double Tlow, double Thigh)
: 	lower_bound(E, Tlow),
	upper_bound(E, Thigh)
{
	assert(Tlow < Thigh);
}

selfjoin_agms_safezone::selfjoin_agms_safezone(selfjoin_query& q) 
:  selfjoin_agms_safezone(q.E, q.Tlow, q.Thigh)
{ }


// from-scratch computation
double selfjoin_agms_safezone::operator()(const sketch& X) 
{
	return min(lower_bound(X), upper_bound(X));
}
double selfjoin_agms_safezone::operator()(const sketch& X, double& zeta_l, double& zeta_u) 
{
	zeta_l = lower_bound(X);
	zeta_u = upper_bound(X);
	return min(zeta_l, zeta_u);
}


double selfjoin_agms_safezone::with_inc(incremental_state& incstate, const sketch& X)
{
	return min(lower_bound.with_inc(incstate.lower, X), 
		upper_bound.with_inc(incstate.upper, X));
}
double selfjoin_agms_safezone::with_inc(incremental_state& incstate, const sketch& X,
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




//
//  Query objects
//


selfjoin_query::selfjoin_query(double _beta, projection _proj)
	: beta(_beta), epsilon(_proj.epsilon()), E(_proj)
{
	assert( norm_Linf(E)==0.0);
	if( epsilon >= beta )
		throw std::invalid_argument("total error is less than sketch error");
	compute();
	assert(fabs(zeta_E-sqrt( (_proj.depth()+1)/2))<1E-15);
}


void selfjoin_query::update_estimate(const sketch& newE)
{
	// compute the admissible region
	E += newE;
	compute();
}

void selfjoin_query::compute()
{
	Qest = dot_est(E);

	if(Qest>0) {
		Tlow = (1+epsilon)*Qest/(1.0+beta);
		Thigh = (1-epsilon)*Qest/(1.0-beta);
		safe_zone = std::move(selfjoin_agms_safezone(*this)); 
	}
	else {
		Tlow = 0.0; Thigh=1.0;
		safe_zone = selfjoin_agms_safezone(E,0.0,1.0);
	}

	zeta_E = safe_zone(E);
}



/////////////////////////////////////////////////////////
//
//  selfjoin_agms_safezone_upper_bound
//  selfjoin_agms_safezone_lower_bound
//  selfjoin_agms_safezone
//
/////////////////////////////////////////////////////////


double hyberbola_nearest_neighbor(double p, double q, double T, double epsilon)
{
	/*
		Consider the hyperbola $y(x) = \sqrt{x^2+T}$ for $x\in [0,+\infty)$, and
		let $ (\xi, \psi) $ be a point on it.

		First, assume $\xi >0$. Then, the normal to the hyperbola at that point
		is the line passing through points $(2\xi,0)$ and $(0,2\psi)$. To see this,
		note that $\nabla y^2-x^2 = (-2x, 2y)$. In other words, the normal to the
		hyperbola at $(\xi, \psi)$ has a parametric equation of 
		\[  (\xi,\psi) + t (-2\xi, 2\psi).  \]

		Now, any point $(p,q)$ whose nearest neighbor is $(\xi, \psi)$ must satisfy
		\[  (p,q) =  (\xi,\psi) + t (-2\xi, 2\psi),  \]
		and by eliminating $t$ we get 
		\[   \frac{p}{\xi} + \frac{q}{\psi} = 2.  \]
		Thus, it suffices to find the root of the function
		\[  g(x) = 2 - p/x - q/y(x) \]
		(which is unique).

		This function has $g(0) = -\infty$, $g(\xi) = 0$ and $g(\max(p,q))>0$. Also,
		\[  g'(x) = \frac{p}{x^2} + \frac{qx}{2y^3}.  \]

		In case $p=0$, it is easy to see that, if $q>2\sqrt{T}$, then the nearest point is
		$(\sqrt{(q/2)^2 - T}, q/2)$, and for $q\leq 2\sqrt{T}$, the answer is $(0, \sqrt{T})$.
	*/
	using std::max;

	if(T<0)
		throw std::invalid_argument("call to hyperbola_nearest_neighbor with T<0");

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

	size_t loops = 80;
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

#undef Y
#undef g
	return xm;
}
