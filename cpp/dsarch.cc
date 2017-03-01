
#include <iostream>
#include <vector>
#include <boost/core/demangle.hpp>

#include "dsarch.hh"
#include "results.hh"
#include "method.hh"

namespace dds{

using namespace std;

//-------------------
//
//  host
//
//-------------------



host::host(basic_network* n, bool _b) 
: _net(n), _addr(unknown_addr), _bcast(_b)
{
	if(!_bcast) {
		_net->_hosts.insert(this);
		_net->all_hosts.join(this);
	}
	else
		_net->_groups.insert(this);
}

host::host(basic_network* n)
: host(n, false)
{ }

host::~host()
{ }


host_addr host::addr() 
{
	if(_addr == unknown_addr) {
		set_addr();
		if(_addr == unknown_addr)
			this->host::set_addr();
	}
	return _addr;
}


bool host::set_addr(host_addr a)
{
	return _net->assign_address(this, a);
}


void host::set_addr()
{
	if(_addr == unknown_addr) 
		_net->assign_address(this, _addr);
	assert(_addr != unknown_addr);
}


//-------------------
//
//  host group
//
//-------------------


host_group::host_group(basic_network* n) 
: host(n, true)
{ }


void host_group::join(host* h)
{
	if(h->is_bcast())
		throw std::logic_error("A host group cannot join another");
	grp.insert(h);
}


//-------------------
//
//  channel
//
//-------------------



channel::channel(host* _src, host* _dst, rpcc_t _rpcc) 
	: src(_src), dst(_dst), rpcc(_rpcc), msgs(0), byts(0) 
{  }


void channel::transmit(size_t msg_size)
{
	msgs++;
	byts += msg_size;
}


//-------------------
//
//  basic network
//
//-------------------




channel* basic_network::connect(host* src, host* dst, rpcc_t endp)
{
	// check for existing channel
	for(auto chan : dst->_incoming) {
		assert(chan->dst == dst);
		if(chan->src == src && chan->rpcc==endp)
			return chan;
	}

	// create new channel
	channel* chan = new channel(src, dst, endp);

	// add it to places
	_channels.insert(chan);
	dst->_incoming.insert(chan);

	return chan;
}


rpcc_t basic_network::decl_interface(const std::type_info& ti)
{
	return decl_interface(type_index(ti));
}

rpcc_t basic_network::decl_interface(const type_index& tix)
{
	return decl_interface(boost::core::demangle(tix.name()));
}

rpcc_t basic_network::decl_interface(const string& name)
{
	return rpctab.declare(name);
}



rpcc_t basic_network::decl_method(rpcc_t ifc, const string& name, bool onew)
{
	return rpctab.declare(ifc, name, onew);
}



basic_network::basic_network()
: all_hosts(this)
{ 
	new_group_addr = -1;
	new_host_addr = 0;
	all_hosts.set_addr(-1);
}


bool basic_network::assign_address(host* h, host_addr a)
{
	if(h->_addr != unknown_addr)
		throw std::invalid_argument("host already has an address assignd");

	if(a==unknown_addr) {
		// assign a default address
		int step = h->is_bcast() ? -1 : 1;
		host_addr& ap = h->is_bcast() ? new_group_addr : new_host_addr;

		while(h->_addr == unknown_addr) {

			auto it = addr_map.find(ap);
			if(it == addr_map.end()) {
				// found it!
				h->_addr = ap;
				addr_map[ap] = h;
			}

			ap += step;
		}
		return true;
	}


	// check that the address is unassigned
	if(addr_map.find(a)==addr_map.end()) {
		h->_addr = a;
		addr_map[a] = h;
		return true;
	} else {
		return false;
	}
	
}


void basic_network::reserve_addresses(host_addr a)
{
	if(a>=0) {
		if(new_host_addr <= a) new_host_addr = a + 1;
	} else {
		if(new_group_addr>=a) new_group_addr = a - 1;
	}
}



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


//-------------------
//
//  RPC interface
//
//-------------------



static inline size_t __method_index(rpcc_t rpcc) {
	if((rpcc & RPCC_METH_MASK)==0)
		throw std::invalid_argument("invalid method code");
	return ((rpcc & RPCC_METH_MASK)>>1)-1;
}

rpc_interface::rpc_interface() 
{}

rpc_interface::rpc_interface(rpcc_t _c, const string& _n) 
: rpc_obj(_c), named(_n) 
{}

rpcc_t rpc_interface::declare(const string& mname, bool onew) 
{
	auto it = name_map.find(mname);
	if(it!=name_map.end()) {
		rpc_method& method = methods[it->second];
		assert(method.name()==mname);
		if(method.one_way != onew)
			throw std::logic_error("method redeclared with different way");
		return method.rpcc;
	}

	if(methods.size()<<1 == RPCC_METH_MASK)
		throw std::length_error("too many methods in interface");

	if(mname.size()==0)
		throw std::invalid_argument("empty method name");

	rpcc_t mrpcc = rpcc | ((methods.size()+1)<<1);
	name_map[mname] = methods.size();
	methods.emplace_back(mrpcc, mname, onew);
	return mrpcc;
}

const rpc_method& rpc_interface::get_method(rpcc_t rpcc) const 
{
	return  methods.at( __method_index(rpcc) );
}

const size_t rpc_interface::num_channels() const
{
	size_t cno = 0;
	for(auto& m : methods)
		cno += m.num_channels();
	return cno;
}


rpcc_t rpc_interface::code(const string& mname) const
{
	auto it = name_map.find(mname);
	if(it != name_map.end()) {
		return methods[it->second].rpcc;
	}
	return 0;
}



//-------------------
//
//  RPC protocol
//
//-------------------

static inline size_t __ifc_index(rpcc_t rpcc) {
	if((rpcc & RPCC_IFC_MASK)==0)
		throw std::invalid_argument("invalid interface code");
	return (rpcc >> RPCC_BITS_PER_IFC)-1;
}

rpcc_t rpc_protocol::declare(const string& name) 
{
	auto it = name_map.find(name);
	if(it != name_map.end()) {
		return ifaces[it->second].rpcc;
	} 
	if(name.size()==0)
		throw std::invalid_argument("empty interface name");
	// create new
	rpcc_t irpcc = (ifaces.size()+1) << RPCC_BITS_PER_IFC;
	name_map[name] = ifaces.size();
	ifaces.emplace_back(irpcc, name);
	return irpcc;
}

rpcc_t rpc_protocol::declare(rpcc_t ifc, const string& mname, bool onew) 
{
	return ifaces[__ifc_index(ifc)].declare(mname, onew);
}

const rpc_interface& rpc_protocol::get_interface(rpcc_t rpcc) const 
{
	return ifaces.at(__ifc_index(rpcc));
}

const rpc_method& rpc_protocol::get_method(rpcc_t rpcc) const 
{
	return ifaces.at(__ifc_index(rpcc)).get_method(rpcc);
}


rpcc_t rpc_protocol::code(const string& name) const
{
	auto it = name_map.find(name);
	if(it != name_map.end()) {
		return ifaces[it->second].rpcc;
	}
	return 0;
}

rpcc_t rpc_protocol::code(const type_info& ti) const
{
	return code(boost::core::demangle(ti.name()));
}

rpcc_t rpc_protocol::code(const string& name, const string& mname) const
{
	auto it = name_map.find(name);
	if(it != name_map.end()) {
		return ifaces[it->second].code(mname);
	}
	return 0;
}

rpcc_t rpc_protocol::code(const type_info& ti, const string& mname) const
{
	return code(boost::core::demangle(ti.name()), mname);
}



const rpc_protocol rpc_protocol::empty;


//-------------------
//
//  RPC proxy
//
//-------------------




rpc_proxy::rpc_proxy(size_t ifc, host* _own)
: _r_ifc(ifc), _r_owner(_own)
{ 
	assert(! _own->is_bcast()); 
}


rpc_proxy::rpc_proxy(const string& name, host* _own) 
: rpc_proxy(_own->net()->decl_interface(name), _own) 
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


//-------------------
//
//  RPC call
//
//-------------------



rpc_call::rpc_call(rpc_proxy* _prx, bool _oneway, const string& _name)
: _proxy(_prx), 
	_endpoint(_prx->_r_owner->net()->decl_method(_prx->_r_ifc,_name, _oneway)), 
	one_way(_oneway)
{
	_prx->_r_register(this);
}


void rpc_call::connect(host* dst)
{
	basic_network* nw = _proxy->_r_owner->net();
	host* owner = _proxy->_r_owner;

	assert(! dst->is_bcast()); // for now...
	_req_chan = nw->connect(owner, dst, _endpoint);
	if(! one_way)
		_resp_chan = nw->connect(dst, owner, _endpoint | RPCC_RESP_MASK);
}




}

