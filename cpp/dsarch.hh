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

#include "dds.hh"
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


/**
	Hosts are used as nodes in the netork graph
  */
class host : public named
{
protected:
	network* _net;
	std::unordered_map<host*, channel*> rtab_to, rtab_from;
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
	std::unordered_set<host*> _hosts;
public:

	network() { }

	inline const std::unordered_set<host*>& hosts() const { 
		return _hosts; 
	}

	inline size_t size() const { return _hosts.size(); }

	channel* connect(host* src, host* dest);

	inline const std::unordered_map<host*, channel*>& routes_from(host* h) const {
		return h->rtab_to;
	}

	virtual ~network();
	friend class host;
};


/**
	A star network topology.

	In a star network, every regular node (spoke) is connected to 
	a central node (hub).
  */
class star_network : public network
{
protected:
	host* _hub;
public:
	star_network() : _hub(nullptr) { }

	inline void set_hub(host* h) { _hub = h; }
	inline host* hub() const { return _hub; }
};


/**
	A process runs on a host and can have remote methods.
  */
class process : public host
{
public:
	using host::host;
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
	A method that distributes the stream records to sites
 */
template <typename SiteProc>
class stream_mapper_method
{
protected:
	std::unordered_map<source_id, SiteProc*> stream_map;
public:
	void map_site(SiteProc* proc) 
	{
		stream_map[proc->site_id()] = proc;
	}

	void process(const dds_record& rec) override
	{
		stream_map[rec.sid]->handle(rec);
	}
};


/*	----------------------------------------

	RPC for protocols

	--------------------------------------- */



template <typename MsgType>
size_t message_size(const MsgType& m);

template <>
inline size_t message_size<std::string>(const std::string& s)
{
	return s.size();
}


#define MSG_SIZE_SIZEOF(type)\
template<>\
inline size_t message_size<type>(const type& i) { return sizeof(type); }

MSG_SIZE_SIZEOF(int)
MSG_SIZE_SIZEOF(unsigned int)
MSG_SIZE_SIZEOF(long)
MSG_SIZE_SIZEOF(unsigned long)
MSG_SIZE_SIZEOF(double)


template <typename Process>
class remote_proxy 
{
protected:
	process* _owner;
	Process* _proc;
	channel* _req_chan;
	channel* _resp_chan;
public:
	inline remote_proxy(process* o, Process* p) 
	: _owner(o), _proc(p) 
	{
		_req_chan = _owner->connect(p);
		_resp_chan = p->connect(_owner);
	}
	inline channel* request_channel() const { return _req_chan; }
	inline channel* response_channel() const { return _resp_chan; }
	inline Process* proc() const { return _proc; }
	inline process* owner() const { return _owner; }
};


/**
	Marker type for remote methods
  */
struct oneway {};

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

template  <typename Dest, typename Request, typename Response>
struct remote_method_base : proxy_method<Dest>
{
	typedef	Response (Dest::* method_type)(Request);
	method_type method;

	remote_method_base(remote_proxy<Dest>* _proxy, method_type _meth)
	: proxy_method<Dest>(_proxy), method(_meth) {}
};

template  <typename Dest, typename Response>
struct remote_method_base<Dest, void, Response> : proxy_method<Dest>
{
	typedef	Response (Dest::* method_type)();
	method_type method;

	remote_method_base(remote_proxy<Dest>* _proxy, method_type _meth)
	: proxy_method<Dest>(_proxy), method(_meth) {}
};


template <typename Dest, typename Request, typename Response>
struct remote_method : remote_method_base<Dest, Request, Response>
{
	using remote_method_base<Dest, Request, Response>::remote_method_base;

	inline Response operator()(Request s) const
	{
		this->transmit_request(message_size(s));
		Response r = (this->proxy->proc()->* (this->method))(s);
		this->transmit_response(message_size(r));
		return r;
	}
};

template <typename Dest, typename Response>
struct remote_method<Dest, void, Response> 
	: remote_method_base<Dest, void, Response>
{
	using remote_method_base<Dest, void, Response>::remote_method_base;

	inline Response operator()() const
	{
		this->transmit_request(0);
		Response r = (this->proxy->proc()->*(this->method))();
		this->transmit_response(message_size(r));
		return r;
	}
};


template <typename Dest, typename Request>
struct remote_method<Dest, Request, void>
	: remote_method_base<Dest, Request, void>
{
	using remote_method_base<Dest, Request, void>::remote_method_base;

	inline void operator()(Request s) const
	{
		this->transmit_request(message_size(s));
		(this->proxy->proc()->*(this->method))(s);
		this->transmit_response(0);
	}
};


template <typename Dest>
struct remote_method<Dest, void, void>
	: remote_method_base<Dest, void, void>
{
	using remote_method_base<Dest, void, void>::remote_method_base;

	inline void operator()() const
	{
		this->transmit_request(0);
		(this->proxy->proc()->*(this->method))();
		this->transmit_response(0);
	}
};


template <typename Dest, typename Request>
struct remote_method<Dest, Request, oneway>
	: remote_method_base<Dest, Request, void>
{
	using remote_method_base<Dest, Request, void>::remote_method_base;

	inline void operator()(Request s) const
	{
		this->transmit_request(message_size(s));
		(this->proxy->proc()->*(this->method))(s);
	}
};


template <typename Dest>
struct remote_method<Dest, void, oneway>
	: remote_method_base<Dest, void, void>
{
	using remote_method_base<Dest, void, void>::remote_method_base;

	inline void operator()() const
	{
		this->transmit_request(0);
		(this->proxy->proc()->*(this->method))();
	}
};



} // end namespace dss



#endif
