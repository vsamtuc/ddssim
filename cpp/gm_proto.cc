
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
safezone::safezone(safezone_func* sz)
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


//////////////////////////////////////
//
// Creation of query 
//
//////////////////////////////////////




continuous_query* gm::create_continuous_query(const Json::Value& js)
{
	// these are compulsory items
	qtype qt = qtype_repr[js["query"].asString()];
    vector<stream_id> sids = get_streams(js);
    projection proj = get_projection(js);
    double beta = js["beta"].asDouble();

    protocol_config cfg = get_protocol_config(js);

    switch(qt)
    {
    	case qtype::SELFJOIN:
    		return new agms_continuous_query< 
    				agms_join_query_state<qtype::SELFJOIN, selfjoin_agms_safezone>
    			>
    			(sids, proj, beta, qtype::SELFJOIN, cfg);

    		//return new agms_continuous_query<selfjoin_query_state, 1>(sids, proj, beta, qt);

    	case qtype::JOIN:
    		return new agms_continuous_query< 
    				agms_join_query_state<qtype::JOIN, twoway_join_agms_safezone>
    			>
    			(sids, proj, beta, qtype::JOIN, cfg);
    		//return new agms_continuous_query<twoway_join_query_state, 2>(sids, proj, beta, qt);

    	default:
    		throw std::runtime_error("Could not process the query type");
    }

}



//////////////////////////////////////
//
// gm_comm_results_t  table
//
//////////////////////////////////////



gm_comm_results_t::gm_comm_results_t(const string& name) 
    : result_table(name),
        dataset_results(this),
        comm_results(this)
{
}

gm_comm_results_t gm::gm_comm_results;


//////////////////////////////////////
//
// tcp_channel
//
//////////////////////////////////////


tcp_channel::tcp_channel(host* src, host* dst, rpcc_t endp)
    : channel(src, dst, endp), tcp_byts(0)
{ }


void tcp_channel::transmit(size_t msg_size)
{
    // update parent statistics
    channel::transmit(msg_size);

    // update tcp byte count
    size_t segno = (msg_size + tcp_mss - 1)/tcp_mss;
    tcp_byts += msg_size + segno * tcp_header_bytes;
}



//////////////////////////////////////
//
// Protocol configuration
//
//////////////////////////////////////

enum_repr<rebalancing> gm::rebalancing_repr ({
    { rebalancing::none, "none" },
    { rebalancing::random, "random" },
    { rebalancing::random_limits, "random_limits" }
});


protocol_config gm::get_protocol_config(const Json::Value& js)
{
	protocol_config cfg;

	cfg.use_cost_model = js.get("use_cost_model", cfg.use_cost_model).asBool();
	cfg.eikonal = js.get("eikonal", cfg.eikonal).asBool();
    cfg.rebalance_algorithm = rebalancing_repr[js.get("rebalancing", "random_limits").asString()];

	return cfg;
}


