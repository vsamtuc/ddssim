#ifndef __RESULTS_HH__
#define __RESULTS_HH__

#include "dsarch.hh"
#include "output.hh"
#include "method.hh"

namespace dds {


/**
	Statistics mixin for the metadata of the data source.
  */
struct dataset_results : private reactive
{
	column<string> dset_name        { "dset_name", 64, "%s" };
	column<timestamp> dset_window   { "dset_window", "%d" };
	column<size_t> dset_warmup      { "dset_warmup", "%zu" };
	column<size_t> dset_size        { "dset_size", "%zu" };	
	column<timestamp> dset_duration { "dset_duration", "%ld" };
	column<size_t> dset_streams		{ "dset_streams", "%zu"};
	column<size_t> dset_hosts		{ "dset_hosts", "%zu"};
	column<size_t> dset_bytes		{ "dset_bytes", "%zu"};

	dataset_results(result_table* table);
	void fill();
};


/**
	Statistics mixin for total network traffic.
  */
struct comm_results 
{
	column<size_t> total_msg 	{"total_msg", "%zu" };
	column<size_t> total_bytes 	{"total_bytes", "%zu" };
	column<double> traffic_pct  {"traffic_pct", "%.10g" };

	comm_results(result_table* table);
	void fill(basic_network* nw);
};


	

/**
	Communication results for each network
  */
struct network_comm_results_t : result_table, comm_results
{
	column<string> netname   	{this, "netname", 64, "%s" };
	column<string> protocol     {this, "protocol", 64, "%s" };
	column<size_t> size         {this, "size", "%zu"};

	network_comm_results_t() 
		: result_table("network_comm_results"), comm_results(this) {}
	network_comm_results_t(const string& name) 
		: result_table(name), comm_results(this) {}
	void fill_columns(basic_network* nw);
};
extern network_comm_results_t network_comm_results;




struct network_host_traffic_t : result_table
{
	// each row corresponds to a channel
	column<string> netname		{this, "netname", 64, "%s"};
	column<string> protocol   	{this, "protocol", 64, "%s" };
	column<host_addr> src 		{this, "src", "%d"};
	column<host_addr> dst 		{this, "dst", "%d"};
	column<rpcc_t>  endp		{this, "endp", "%u"};
	column<size_t>  msgs		{this, "msgs", "%zu"};
	column<size_t>  bytes		{this, "bytes", "%zu"};
	network_host_traffic_t() : result_table("network_host_traffic") {}
	void output_results(basic_network* nw);	
};
extern network_host_traffic_t network_host_traffic;

struct network_interfaces_t : result_table
{
	// each row corresponds to a host interface
	column<string> netname		{this, "netname", 64, "%s"};
	column<string> protocol   	{this, "protocol", 64, "%s" };
	column<rpcc_t> rpcc 		{this, "rpcc", "%hu"};
	column<string> iface 		{this, "iface", 64, "%s"};
	column<string> method 		{this, "method", 64, "%s"};
	column<bool>   oneway		{this, "oneway", "%d"};
	network_interfaces_t() : result_table("network_interfaces") {}
	void output_results(basic_network* nw);	
};
extern network_interfaces_t network_interfaces;

} // end namespace dds

#endif
