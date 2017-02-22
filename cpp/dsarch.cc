
#include <iostream>
#include <vector>
#include "dsarch.hh"
#include "results.hh"


namespace dds{

using namespace std;

host::host(network* n) : _net(n) 
{
	_net->_hosts.insert(this);
}

channel* host::connect(host* dest)
{
	return _net->connect(this, dest);
}

host::~host()
{
}


channel::channel(host* s, host* d) 
	: src(s), dst(d), msgs(0), byts(0) 
{	
	src->rtab_to[dst] = this;
	dst->rtab_from[src] = this;
}


void channel::transmit(size_t msg_size)
{
	msgs++;
	byts += msg_size;
}


channel* network::connect(host* src, host* dst)
{
	auto x = src->rtab_to.find(dst);
	if(x!=src->rtab_to.end())
		return (*x).second;
	else {
		channel* ret = new channel(src,dst);
		return ret;
	}
}

network::~network()
{
	for(auto h : _hosts) {
		for(auto ch : h->rtab_to) 
			delete ch.second;
		delete h;
	}
}

void network::comm_results_fill_in()
{
	size_t total_msg = 0;
	size_t total_bytes = 0;
	for(auto h : _hosts) {
		for(auto ch : h->rtab_to) 
		{
			channel* c = ch.second;
			total_msg += c->messages();
			total_bytes += c->bytes();
		}
	}
	comm_results.total_msg = total_msg;
	comm_results.total_bytes = total_bytes;

	double stream_bytes = sizeof(dds_record)* CTX.stream_count();
	comm_results.traffic_pct = total_bytes/stream_bytes;
}


}

