#ifndef __DSARCH_HH__
#define __DSARCH_HH__

/**
	\file Distributed stream system architecture simulation classes.

	The purpose of these classes is to collect detailed statistics
	that are independent of the particular algorithm, and report
	them in a standardized (and therefore auto-processable) manner.
  */

#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <cassert>
#include <typeinfo>
#include <typeindex>
#include <utility>
#include <type_traits>

#include "dds.hh"
#include "agms.hh"
#include "output.hh"
#include "method.hh"

namespace dds {


class basic_network;
class host;
class host_group;
class process;
class channel;


enum class chan_dir { REQ, RSP, BCAST };

/**
	Point-to-point or broadcast unidirectional channel.

	Channels are used to collect network statistics. Each 
	channel counts the number of messages and the total message
	size. A channel is defined by
	- the source host
	- the destination host
	- the type of message

	Message types are defined by an RPC (message) code. Each rpc endpoint
	(method) is associated with a request channel, and---if it is
	not oneway---with a response channel.

	Therefore, assuming that we have \f$ m\f$ host types and for each host
	type \f$ i \f$ there are \f$ n_i \f$ hosts in the network, and further
	that the in-degree (number of nodes that call it) of each host of type
	\f$i\f$ is \f$d_i\f$, and finally that a host of type \f$i\f$ has 
	\f$ q_i \f$ methods in its interface, there is a total of 
	\f[  \prod_{i=1}^m n_i d_i q_i  \f] 
	request channels in the network, and up to that many response channels.

	Finally, a channel can be a broadcast channel. This is always associated
	with a request for come rpc method and a special destination host,
	which is marked as a broadcast host.

	Note that the source host cannot be a broadcast host.
  */
class channel
{
protected:
	host *src, *dst;
	size_t rpcc;
	chan_dir dir;

	size_t msgs, byts;

	channel(host *s, host* d, size_t rpcc, chan_dir dir);
public:

	inline host* source() const { return src; }
	inline host* destination() const { return dst; }
	inline size_t rpc_code() const { return rpcc; }
	inline chan_dir direction() const { return dir; }
	inline auto messages() const { return msgs; }
	inline auto bytes() const { return byts; }

	void transmit(size_t msg_size);
	friend class basic_network;
};



typedef std::unordered_map<host*, channel*> channel_map;
typedef std::unordered_set<channel*> channel_set;
typedef std::unordered_set<host*> host_set;


/**
	Hosts are used as addresses in the basic_network.

	Hosts can be named, for more friendly output. A host
	can represent a single network destination (site), or a set
	of network destinations.

	Any subclass of host is a single site. For broadcast sites,
	one has to use `host_group`.

	@see host_group
  */
class host : public named
{
	host(basic_network* n, bool _bcast);
	friend class host_group;
	friend class basic_network;
protected:
	basic_network* _net;
	bool _bcast;
	channel_set _incoming;
	virtual ~host();
public:
	host(basic_network* n);
	
	inline basic_network* net() const { return _net; }
	inline bool is_bcast() const { return _bcast; }

	friend class channel;
	friend class basic_network;
};


/**
	A host group represents a broadcast address.

	To build the membership of a group, one has to
	add (non-group) hosts via method `join`.
  */
class host_group : public host
{
protected:
	host_set grp;
	friend class basic_network;
public:
	host_group(basic_network* nw);

	/**
		Add a simple host to this group.
	  */
	void join(host* h);
};




using std::type_index;




struct rpc_call;

/**
	An rpc proxy represents a proxy object for some host.

	We do not want to pollute the class namespace, in order
	to avoid collisions with method names.
  */
struct rpc_proxy
{
	size_t _r_ifc;
	std::vector<rpc_call*> _r_calls;
	host* _r_owner;
	host* _r_proc = nullptr;

	rpc_proxy(const string& name, host* _own);
	size_t _r_register(rpc_call* call);
	void _r_connect(host* dst);
private:
	template <typename Dest>
	friend class remote_proxy;
	rpc_proxy(size_t ifc, host* _own);
};


/**
	An rcp method belongs to some specific rpc proxy.
  */
struct rpc_call
{
protected:
	rpc_proxy* _proxy;
	size_t _endpoint;
	channel* _req_chan = nullptr;
	channel* _resp_chan = nullptr;
	bool one_way;
public:
	rpc_call(rpc_proxy* _prx, bool _oneway, const string& _name);

	void connect(host* dst);

	inline size_t endpoint() const { return _endpoint; }
	inline channel* request_channel() const { return _req_chan; }
	inline channel* response_channel() const { return _resp_chan; }
};


/**
	A collection of hosts and channels.

	This class manages the network elements: hosts, groups,
	channels, rpc endpoints.
  */
class basic_network : public named
{
protected:
	host_set _hosts;		// all the simple hosts
	host_set _groups;		// all the host groups
	channel_set _channels;	// all the channels

	//std::unordered_map<type_index, string>  rpc_type_label; // rpc interface
	size_t rpc_ifc_uuid;    // used to generate unique ifc codes
	std::unordered_map<string, size_t> rpc_ifc;
	std::unordered_map<size_t, string> ifc_rpc;

	std::unordered_map<size_t, size_t> rpcc_uuid;
	std::map< std::pair<size_t, string>, size_t> rmi_rpcc;
	std::unordered_map< size_t, std::pair<size_t, string>> rpcc_rmi;

	bool is_legal_ifc(size_t);
	bool is_legal_rpcc(size_t);
public:

	basic_network();

	inline const host_set& hosts() const { 
		return _hosts; 
	}

	inline size_t size() const { return _hosts.size(); }

	size_t get_ifc(const std::type_info& ti);
	size_t get_ifc(const std::type_index& tix);
	size_t get_ifc(const string&);

	size_t get_rpcc(size_t ifc, const string&);

	string get_rpcc_name(size_t rpcc);

	channel* connect(host* src, host* dest, size_t rpcc, chan_dir dir);

	virtual ~basic_network();
	friend class host;

	// fill the traffic entries in comm_results
	void comm_results_fill_in();

};


/**
	A process extends a host with remote methods.
  */
class process : public host
{
public:
	using host::host;

	virtual void setup_connections() { }
};


/**
   A local site accepts the input from streams.
   */
class local_site : public process
{
protected:
	source_id sid=-1;

public:
	local_site(basic_network* nw, source_id _sid) 
	: process(nw), sid(_sid) 
	{}

	inline source_id site_id() const { return sid; }

	void handle(const dds_record& rec) {}
};


/**
	A star network topology.

	In a star network, every regular node (site) is connected to 
	a central node (hub).

	Nodes in a star network are local sites.

	This class is a mixin
  */
template <typename Net, typename Hub, typename Site>
struct star_network : public basic_network
{
	Hub* hub;
	std::unordered_map<source_id, Site*> sites;

	star_network() : hub(nullptr) { }

	template <typename ... Args>
	Net* setup(Args...args)
	{
		// create the nodes
		hub = new Hub((Net*)this, args...);

		for(auto hid : CTX.metadata().source_ids()) 
		{
			Site* n = new Site((Net*)this, hid, args...);
			sites[hid] = n;
		}

		// make the connections
		hub->setup_connections();
		for(auto n : sites) {
			n.second->setup_connections();
		}

		return (Net*)this;
	}
};


/*	----------------------------------------

	Typed RPC for protocols

	--------------------------------------- */


/**
	Marker type for remote methods
  */
struct oneway {};

template <>
inline size_t byte_size<oneway>(const oneway& s) { return 0; }


/**
	Marker type for remote methods that 
	'return void' (N.B.: remote methods cannot 
	return void)
  */
struct ACK {};

template <>
inline size_t byte_size<ACK>(const ACK& s) { return 0; }


template <typename T>
struct msgwrapper
{
	T* payload;
	msgwrapper(T* _p) : payload(_p){}
	inline size_t byte_size() const { return byte_size((const T&) *payload); }
};
template <typename T>
msgwrapper<T> wrap(T* p) { return msgwrapper<T>(p); }
template <typename T>
msgwrapper<T> wrap(T& p) { return msgwrapper<T>(&p); }




/**
	Compute the message size of an argument list.

	This function simply adds the \c byte_size() value
	of each argument.
  */
template <typename...Args>
inline size_t message_size(const Args& ...args)
{
	size_t total = 0;
	typedef size_t swallow[];
	(void) swallow {0,
	 	(total+= byte_size(args))...
	};
	return total;
}

template <typename Dest>
struct proxy_method;

template <typename Dest, typename Response, typename...Args>
struct remote_method;

template <typename Process>
class remote_proxy : public rpc_proxy
{
public:
	typedef Process proxied_type;

	inline remote_proxy(process* owner) 
	: rpc_proxy(owner->net()->get_ifc(typeid(Process)), owner)
	{ }

	inline void operator<<=(Process* dest) {
		_r_connect(dest);
	}

	inline Process* proc() const { return (Process*) _r_proc; }
};


template <typename Dest>
struct proxy_method  : rpc_call
{
	typedef remote_proxy<Dest> proxy_type;
	proxy_type* proxy;

	inline proxy_method(proxy_type* _proxy, bool one_way, const string& _name) 
	: rpc_call(_proxy, one_way, _name), proxy(_proxy) {}

	inline void transmit_request(size_t msg_size) const {
		this->request_channel()->transmit(msg_size);		
	}

	inline void transmit_response(size_t msg_size) const {
		this->response_channel()->transmit(msg_size);		
	}	
};


/*
	Typed wrapper for remote sites
 */

template <typename T>
struct context_wrapper {
	T const arg;
	explicit context_wrapper(T _arg) : arg(_arg) { }
	size_t byte_size() const { return 0; }
};

template <typename U>
U context_unwrap(U&& x) { return x; }

template <typename U>
U context_unwrap(context_wrapper<U>&& wrapper) { return wrapper.arg; }

template <typename U>
context_wrapper<U> by_context(U x) { return context_wrapper<U>(x); }


template <typename Dest, typename Response, typename ... Args>
struct remote_method : proxy_method<Dest>
{
	typedef	Response (Dest::* method_type)(Args...);
	method_type method;

	remote_method(remote_proxy<Dest>* _proxy, method_type _meth, const string& _name)
	: proxy_method<Dest>(_proxy, std::is_same<Response, oneway>::value, _name), 
		method(_meth) 
	{ }

	inline Response operator()(Args...args) const
	{
		this->transmit_request(message_size(args...));
		Response r = (this->proxy->proc()->* (this->method))(
			std::forward<Args>(context_unwrap(args))...);
		if(! std::is_same<Response, oneway>::value )
			this->transmit_response(message_size(r));
		return r;
	}
};

template <typename T, typename Response, typename...Args>
inline remote_method<T, Response, Args...> 
make_remote_method(remote_proxy<T>* owner, Response (T::*method)(Args...))
{
	return remote_method<T, Response, Args...>(owner, method);
}

#define REMOTE_METHOD(RClass, RMethod)\
 decltype(dds::make_remote_method((remote_proxy<RClass>*)nullptr,\
 	&RClass::RMethod)) RMethod { this, &RClass::RMethod, #RMethod }



/*	----------------------------------------

	Messages for common types

	--------------------------------------- */


struct compressed_sketch
{
	const agms::sketch& sk;
	size_t updates;

	size_t byte_size() const {
		size_t E_size = dds::byte_size(sk);
		size_t Raw_size = sizeof(dds_record)*updates;
		return std::min(E_size, Raw_size);
	}
};




} // end namespace dss



#endif
