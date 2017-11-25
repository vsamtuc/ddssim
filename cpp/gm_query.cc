
#include "gm_query.hh"


using namespace gm;


/////////////////////////////////////////////////////////
//
//  agms_query_state
//
/////////////////////////////////////////////////////////

agms_query_state::agms_query_state(double _beta, projection _proj, size_t arity)
	: query_state(arity*_proj.size()),
	  beta(_beta), proj(_proj), 
	  epsilon(_proj.epsilon())
{
	assert(norm_Linf(E)==0.0);
	if( epsilon >= beta )
		throw std::invalid_argument("total error is less than sketch error");
}


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

