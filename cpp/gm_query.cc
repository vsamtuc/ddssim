
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
		Tlow =  Qest - (beta-epsilon)*fabs(Qest)/(1.0+beta);
		Thigh = Qest + (beta-epsilon)*fabs(Qest)/(1.0-beta);
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



double selfjoin_query_state::zeta(const Vec& x)
{
	return safe_zone(x);
}


safezone_func_wrapper* selfjoin_query_state::safezone()
{
	return new std_safezone_func_wrapper<selfjoin_agms_safezone>(safe_zone, E.size(), E);
}

safezone_func_wrapper* selfjoin_query_state::radial_safezone()
{
	// IF EIKONAL
	return new ball_safezone(this);
}


#if 0
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
#endif

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
	auto x1 = x0 + x.size()/2;
	auto x2 = x1 + x.size()/2;
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

double twoway_join_query_state::zeta(const Vec& x)
{
	return safe_zone(x);
}


safezone_func_wrapper* twoway_join_query_state::safezone()
{
	return new std_safezone_func_wrapper<twoway_join_agms_safezone>(safe_zone, E.size(), E);
}

safezone_func_wrapper* twoway_join_query_state::radial_safezone()
{
	// IF EIKONAL
	return new ball_safezone(this);
}


#if 0
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
#endif


/////////////////////////////////////////////////////////
//
//  ball_safezone
//
/////////////////////////////////////////////////////////



void* ball_safezone::alloc_incstate()
{
	return new double;
}

void ball_safezone::free_incstate(void* ptr)
{
	delete static_cast<double*>(ptr);
}

double ball_safezone::compute_zeta(void* inc, const delta_vector& dU, const Vec& U) 
{
	double* incstate = static_cast<double*>(inc);
	return zeta_E() - norm_L2_inc(*incstate, dU);
}

double ball_safezone::compute_zeta(void* inc, const Vec& U) 
{
	double* incstate = static_cast<double*>(inc);
	return zeta_E() - norm_L2_with_inc(*incstate, U);
}

double ball_safezone::compute_zeta(const Vec& U) 
{
	return zeta_E() - norm_L2(U);
}

size_t ball_safezone::zeta_size() const
{
	return 1;
}

