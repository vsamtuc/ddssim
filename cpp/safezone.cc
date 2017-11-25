
#include "binc.hh"
#include "safezone.hh"

using namespace gm;
using namespace hdv;
using namespace binc;




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
//  twoway_join_agms_safezone
//
/////////////////////////////////////////////////////////



twoway_join_agms_safezone::twoway_join_agms_safezone() 
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



