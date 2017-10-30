
#include "results.hh"
#include "method.hh"
#include "binc.hh"

namespace dds {

local_stream_stats_t local_stream_stats;

network_comm_results_t network_comm_results;
void network_comm_results_t::fill_columns(basic_network* nw)
{
	netname = nw->name();

	size_t total_msg = 0;
	size_t total_bytes = 0;
	for(auto&& c : nw->channels()) {
		total_msg += c->messages();
		total_bytes += c->bytes();
	}
	this->total_msg = total_msg;
	this->total_bytes = total_bytes;

	double stream_bytes = sizeof(dds_record)* CTX.stream_count();
	traffic_pct = total_bytes/stream_bytes;	
}


gm_comm_results_t gm_comm_results;
void gm_comm_results_t::fill_columns(basic_network* nw)
{
	protocol = nw->name();

	size_t total_msg = 0;
	size_t total_bytes = 0;
	for(auto&& c : nw->channels()) {
		total_msg += c->messages();
		total_bytes += c->bytes();
	}
	this->total_msg = total_msg;
	this->total_bytes = total_bytes;

	double stream_bytes = sizeof(dds_record)* CTX.stream_count();
	traffic_pct = total_bytes/stream_bytes;	
}


  
network_host_traffic_t network_host_traffic;
void  network_host_traffic_t::output_results(basic_network* nw)
{
	netname = nw->name();
	for(auto&& c : nw->channels()) {
		src = c->source()->addr();
		dst = c->destination()->addr();
		endp = c->rpc_code();
		msgs = c->messages();
		bytes = c->bytes();
		emit_row();
	}	
}


network_interfaces_t network_interfaces;
void  network_interfaces_t::output_results(basic_network* nw)
{
	netname =nw->name();
	for(auto&& ifc : nw->rpc().ifaces) {
		iface = ifc.name();
		for(auto&& meth : ifc.methods) {
			rpcc = meth.rpcc;
			method = meth.name();
			oneway = meth.one_way;
			emit_row();
		}
	}
}


} // end namespace dds
