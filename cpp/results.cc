#include "results.hh"


namespace dds {


local_stream_stats_t lsstats;
local_stream_stats_t::local_stream_stats_t()
: result_table("local_stream_stats")
{
	add({
		&sid,
		&hid,
		&stream_len
	});
}

comm_results_t comm_results;
comm_results_t::comm_results_t()
: result_table("comm_results")
{
	add({
		&netname,
		&max_error,
		&sites,
		&streams,
		&local_viol,
		&total_msg,
		&total_bytes,
		&traffic_pct
	});
}



} // end namespace dds