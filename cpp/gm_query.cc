
#include "gm_query.hh"


using namespace gm;



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

double selfjoin_query_state::zeta(const Vec& x)
{
	return safe_zone(x);
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
//  twoway_join_query_state
//
/////////////////////////////////////////////////////////



twoway_join_query_state::twoway_join_query_state(double _beta, projection _proj)
    : query_state(2*_proj.size()),
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
            Tlow = -1.0; Thigh=1.0;
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

double twoway_join_query_state::zeta(const Vec& x)
{
	return safe_zone(x);
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


