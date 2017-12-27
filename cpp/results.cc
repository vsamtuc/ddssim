
#include "results.hh"
#include "method.hh"
#include "binc.hh"

namespace dds {

dataset_results::dataset_results(result_table* table) 
	:  eca::reactive(&CTX)
{
	table->add({&dset_name, &dset_window, &dset_warmup,
		&dset_size, &dset_duration, &dset_streams,
		&dset_hosts, &dset_bytes
	 });
	on(START_STREAM, [&]() { fill(); });
}

void dataset_results::fill() 
{ 
	dset_name = CTX.metadata().name();
	dset_window = CTX.metadata().window();
	dset_warmup = CTX.metadata().warmup_time()+CTX.metadata().warmup_size();

	dset_size = CTX.metadata().size();
	dset_duration = CTX.metadata().duration();
	dset_streams = CTX.metadata().stream_ids().size();
	dset_hosts = CTX.metadata().source_ids().size();
	dset_bytes = CTX.metadata().size() * sizeof(dds_record);
}



comm_results::comm_results(result_table* table) 
{
	table->add({&total_msg, &total_bytes, &traffic_pct});
}

void comm_results::fill(basic_network* nw)
{
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


// Global result_table objects
network_comm_results_t network_comm_results;
network_host_traffic_t network_host_traffic;
network_interfaces_t network_interfaces;




void network_comm_results_t::fill_columns(basic_network* nw)
{
	netname = nw->name();
	protocol = nw->rpc().name();
	fill(nw);
}

  
void  network_host_traffic_t::output_results(basic_network* nw)
{
	netname = nw->name();
	protocol = nw->rpc().name();
	for(auto&& c : nw->channels()) {
		src = c->source()->addr();
		dst = c->destination()->addr();
		endp = c->rpc_code();
		msgs = c->messages();
		bytes = c->bytes();
		emit_row();
	}	
}


void  network_interfaces_t::output_results(basic_network* nw)
{
	netname =nw->name();
	protocol = nw->rpc().name();
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
