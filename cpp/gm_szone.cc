#include "gm_szone.hh"

using namespace gm;

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

query_state::~query_state()
{ }


double query_state::compute_zeta(const Vec& U) 
{ 
	return zeta(U+E);
}

safezone_func* query_state::radial_safezone()
{
	return nullptr;
}


/////////////////////////////////////////////////////////
//
//  safezone_func
//
/////////////////////////////////////////////////////////

safezone_func::~safezone_func() 
{ }

