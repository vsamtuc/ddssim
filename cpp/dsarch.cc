
#include <iostream>
#include <vector>
#include "dsarch.hh"
#include "results.hh"


namespace dds{

using namespace std;

host::host(basic_network* n, bool _b) 
: _net(n), _bcast(_b)
{
	if(!_bcast) 
		_net->_hosts.insert(this);
	else
		_net->_groups.insert(this);
}

host::host(basic_network* n)
: host(n, false)
{ }

host::~host()
{ }

host_group::host_group(basic_network* n) 
: host(n, true)
{ }


void host_group::join(host* h)
{
	if(h->is_bcast())
		throw std::logic_error("A host group cannot join another");
	grp.insert(h);
}


channel::channel(host* _src, host* _dst, size_t _rpcc, chan_dir _dir) 
	: src(_src), dst(_dst), rpcc(_rpcc), dir(_dir), msgs(0), byts(0) 
{  }


void channel::transmit(size_t msg_size)
{
	msgs++;
	byts += msg_size;
}


channel* basic_network::connect(host* src, host* dst, size_t endp, chan_dir dir)
{
	// check for existing channel
	for(auto chan : dst->_incoming) {
		assert(chan->dst == dst);
		if(chan->src == src && chan->rpcc==endp && chan->dir==dir)
			return chan;
	}

	// create new channel
	channel* chan = new channel(src, dst, endp, dir);

	// add it to places
	_channels.insert(chan);
	dst->_incoming.insert(chan);


	return chan;
}


size_t basic_network::get_ifc(const std::type_info& ti)
{
	return get_ifc(type_index(ti));
}

size_t basic_network::get_ifc(const type_index& tix)
{
	return get_ifc(string(tix.name()));
}

size_t basic_network::get_ifc(const string& name)
{
	if(rpc_ifc.find(name)!=rpc_ifc.end()) {
		return rpc_ifc[name];
	}

	// new ifc, create index entry
	rpc_ifc_uuid += 1000;
	size_t ifcode = rpc_ifc_uuid;
	rpc_ifc[name] = ifcode;
	ifc_rpc[ifcode] = name;
	rpcc_uuid[ifcode] = 0;
	return ifcode;
}


bool basic_network::is_legal_ifc(size_t ifc)
{
	return ifc_rpc.find(ifc)!=ifc_rpc.end();
}

bool basic_network::is_legal_rpcc(size_t rpcc)
{
	return rpcc_rmi.find(rpcc)!=rpcc_rmi.end();
}

string basic_network::get_rpcc_name(size_t rpcc)
{
	if(! is_legal_rpcc(rpcc))
		throw std::invalid_argument("invalid rpcc");
	auto rmi = rpcc_rmi[rpcc];
	return ifc_rpc[rmi.first]+"::"+rmi.second;
}


size_t basic_network::get_rpcc(size_t ifc, const string& name)
{
	std::pair<size_t, string> rmi = std::make_pair(ifc, (string)name);

	if(! is_legal_ifc(ifc)) 
		throw std::invalid_argument("illegal interface code");

	if(rmi_rpcc.find(rmi)!=rmi_rpcc.end()) {
		return rmi_rpcc[rmi];
	}

	if(rpcc_uuid[ifc]>=999)
		throw std::length_error("too many methods in interface");

	// make new entry
	size_t rpcc = ++ rpcc_uuid[ifc];
	rmi_rpcc[rmi] = rpcc;
	rpcc_rmi[rpcc] = rmi;
	return rpcc;
}



basic_network::basic_network()
: rpc_ifc_uuid(0)
{ }


basic_network::~basic_network()
{ }

void basic_network::comm_results_fill_in()
{
	size_t total_msg = 0;
	size_t total_bytes = 0;
	for(auto h : _hosts) {
		for(auto c : h->_incoming) 
		{
			total_msg += c->messages();
			total_bytes += c->bytes();
		}
	}
	comm_results.total_msg = total_msg;
	comm_results.total_bytes = total_bytes;

	double stream_bytes = sizeof(dds_record)* CTX.stream_count();
	comm_results.traffic_pct = total_bytes/stream_bytes;
}


rpc_proxy::rpc_proxy(size_t ifc, host* _own)
: _r_ifc(ifc), _r_owner(_own)
{ 
	assert(! _own->is_bcast()); 
}


rpc_proxy::rpc_proxy(const string& name, host* _own) 
: rpc_proxy(_own->net()->get_ifc(name), _own) 
{ }

size_t rpc_proxy::_r_register(rpc_call* call) {
	_r_calls.push_back(call);
	if(_r_calls.size()>=1000)
		throw std::runtime_error("proxy size too large (exceeds 1000 calls)");
	return _r_calls.size();
}

void rpc_proxy::_r_connect(host* dst)
{
	assert(dst != _r_owner);
	_r_proc = dst;
	for(auto call : _r_calls) {
		call->connect(dst);
	}
}


rpc_call::rpc_call(rpc_proxy* _prx, bool _oneway, const string& _name)
: _proxy(_prx), 
	_endpoint(_prx->_r_owner->net()->get_rpcc(_prx->_r_ifc,_name)), 
	one_way(_oneway)
{
	_prx->_r_register(this);
}


void rpc_call::connect(host* dst)
{
	basic_network* nw = _proxy->_r_owner->net();
	host* owner = _proxy->_r_owner;

	assert(! dst->is_bcast()); // for now...
	_req_chan = nw->connect(owner, dst, _endpoint, chan_dir::REQ);
	if(! one_way)
		_resp_chan = nw->connect(dst, owner, _endpoint, chan_dir::RSP);
}




}

