#ifndef __DSARCH_HH__
#define __DSARCH_HH__

/**
	\file Distributed stream system architecture simulation classes.
  */

#include <unordered_set>
#include <unordered_map>
#include <string>
#include <cstring>
#include <iostream>
#include <cassert>
#include <type_traits>

#include "dds.hh"
#include "output.hh"
#include "method.hh"

namespace dds {


class network;
class host;
class process;
class channel;

/**
	Point-to-point unidirectional channel.

	Channels are mainly used to collect network statistics
  */
class channel
{
protected:
	host *src, *dst;
	size_t msgs, byts;

	channel(host *s, host* d);
public:

	inline host* source() const { return src; }
	inline host* destination() const { return dst; }
	inline auto messages() const { return msgs; }
	inline auto bytes() const { return byts; }

	void transmit(size_t msg_size);
	friend class network;
};


typedef std::unordered_map<host*, channel*> channel_map;
typedef std::unordered_set<host*> host_set;


/**
	Hosts are used as nodes in the netork graph
  */
class host : public named
{
protected:
	network* _net;
	channel_map rtab_to, rtab_from;
	virtual ~host();
public:
	host(network* n);
	
	inline network* net() const { return _net; }

	inline channel* channel_to(host* dest) const { 
		try{
			return rtab_to.at(dest);
		} 
		catch(std::out_of_range) {
			return nullptr;
		}
	}

	channel* connect(host*);

	friend class channel;
	friend class network;
};


/**
	A (directed) graph of hosts and channels.
  */
class network : public named
{
protected:
	host_set _hosts;
public:

	network() { }

	inline const host_set& hosts() const { 
		return _hosts; 
	}

	inline size_t size() const { return _hosts.size(); }

	channel* connect(host* src, host* dest);

	inline const channel_map& routes_from(host* h) const {
		return h->rtab_to;
	}

	virtual ~network();
	friend class host;
};



/**
	A process runs on a host and can have remote methods.
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
	local_site(network* nw, source_id _sid) 
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
struct star_network : public network
{
	Hub* hub;
	std::unordered_map<source_id, Site*> nodes;

	star_network() : hub(nullptr) { }

	template <typename ... Args>
	void setup(Args...args)
	{
		// create the nodes
		hub = new Hub((Net*)this, args...);

		for(auto hid : CTX.metadata().source_ids()) 
		{
			Site* n = new Site((Net*)this, hid, args...);
			nodes[hid] = n;
		}

		// make the connections
		hub->setup_connections();
		for(auto n : nodes) {
			n.second->setup_connections();
		}
	}
};




/*	----------------------------------------

	RPC for protocols

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



template <typename...Args>
inline size_t message_size(Args...args)
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
class remote_proxy 
{
protected:
	process* _owner;
	Process* _proc = nullptr;
	channel* _req_chan = nullptr;
	channel* _resp_chan = nullptr;

	bool _oneway = true;
	template <typename Dest, typename Response, typename...Args>
	friend struct remote_method;
public:

	inline remote_proxy(process* o) 
	: _owner(o)
	{ }

	inline bool is_connected() const { return _proc; }

	inline void connect(Process* p) {
		_proc = p;
		_req_chan = _owner->connect(p);
		if(! _oneway)
			_resp_chan = p->connect(_owner);
		else
			_resp_chan = nullptr;
	}

	inline channel* request_channel() const { return _req_chan; }
	inline channel* response_channel() const { return _resp_chan; }
	inline Process* proc() const { return _proc; }
	inline process* owner() const { return _owner; }
};


template <typename Dest>
struct proxy_method
{
	typedef remote_proxy<Dest> proxy_type;
	proxy_type* proxy;

	inline proxy_method(proxy_type* _proxy) : proxy(_proxy) {}

	inline void transmit_request(size_t msg_size) const {
		proxy->request_channel()->transmit(msg_size);		
	}

	inline void transmit_response(size_t msg_size) const {
		proxy->response_channel()->transmit(msg_size);		
	}	
};


template <typename Dest, typename Response, typename ... Args>
struct remote_method : proxy_method<Dest>
{
	typedef	Response (Dest::* method_type)(Args...);
	method_type method;

	remote_method(remote_proxy<Dest>* _proxy, method_type _meth)
	: proxy_method<Dest>(_proxy), method(_meth) 
	{
		this->proxy->_oneway = this->proxy->_oneway &&
			std::is_same<Response, oneway>::value;
	}

	inline Response operator()(Args...args) const
	{
		this->transmit_request(message_size(args...));
		Response r = (this->proxy->proc()->* (this->method))(args...);
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
 	&RClass::RMethod)) RMethod { this, &RClass::RMethod }




} // end namespace dss



#endif
