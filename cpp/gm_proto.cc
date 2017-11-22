
#include "gm_query.hh"
#include "binc.hh"

using namespace gm;
using namespace dds;

//////////////////////////////////////
//
// safezone
//
//////////////////////////////////////

safezone::safezone() 
: szone(nullptr), inc(nullptr)
{ }

// valid safezone
safezone::safezone(safezone_func_wrapper* sz)
: szone(sz), inc(nullptr)
{
	assert(sz != nullptr);
}

safezone::~safezone()
{
	clear_inc();
}

// Movable
safezone::safezone(safezone&& other)
{
	swap(other);
}

safezone& safezone::operator=(safezone&& other)
{
	swap(other);
	return *this;
}

// Copyable
safezone::safezone(const safezone& other) 
	: inc(nullptr)
{
	szone = other.szone;
}

safezone& safezone::operator=(const safezone& other)
{
	if(szone != other.szone) {
		clear_inc();
		szone = other.szone;
	}
	return *this;
}


namespace gm  {


//////////////////////////////////////
//
// Creation of query 
//
//////////////////////////////////////


continuous_query* create_continuous_query(const Json::Value& js)
{
	qtype qt = qtype_repr[js["query"].asString()];
    vector<stream_id> sids = get_streams(js);
    projection proj = get_projection(js);
    double beta = js["beta"].asDouble();

    switch(qt)
    {
    	case qtype::SELFJOIN:
    		return new agms_continuous_query<selfjoin_query_state, 1>(sids, proj, beta, qt);

    	case qtype::JOIN:
    		return new agms_continuous_query<twoway_join_query_state, 2>(sids, proj, beta, qt);

    	default:
    		throw std::runtime_error("Could not process the query type");
    }

}



} // end namespace gm

