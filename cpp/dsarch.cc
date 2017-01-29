
#include <iostream>
#include <vector>
#include "dsarch.hh"

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



}

