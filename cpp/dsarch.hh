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
#include <typeinfo>
#include <typeindex>
#include <stdexcept>
#include <algorithm>

#include "dds.hh"

namespace dds {


class basic_network;
class host;
class host_group;
class process;
class channel;

/**
	RPC code type.
  */
typedef uint32_t rpcc_t;


constexpr int RPCC_BITS_PER_IFC = 8;
constexpr rpcc_t RPCC_ENDP_MASK = (1<<RPCC_BITS_PER_IFC)-1;
constexpr rpcc_t RPCC_IFC_MASK = ~RPCC_ENDP_MASK;
constexpr rpcc_t RPCC_METH_MASK = RPCC_ENDP_MASK -1;
constexpr rpcc_t RPCC_RESP_MASK = 1;

using std::string;
using std::vector;
using std::unordered_map;
using std::unordered_set;
using std::type_index;
using std::type_info;

/**
	A numeric id for hosts.

	host_addr is used for reporting, therefore the actual
	addresses are user_definable. All that the library does
	is check that the same address is not assigned to 
	multiple hosts. A host's address is finalized when
	the first channel to or from it is created.

	The following are standard assumptions, encoded in the default 
	implementation of `host::set_addr()`

	- For source sites, it is equal to the source_id.
	- For normal hosts the address is non-negative.
	- For host groups it is negative.
	- The "all hosts" group	is -1.

	- The "unknown address" is 2^31-1. Hosts (and groups) are initialized
	to this address.
  */
typedef int32_t host_addr;

constexpr host_addr unknown_addr = std::numeric_limits<host_addr>::max();

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
	rpcc_t rpcc;

	size_t msgs, byts;

	channel(host *s, host* d, rpcc_t rpcc);
public:

	inline host* source() const { return src; }
	inline host* destination() const { return dst; }
	inline rpcc_t rpc_code() const { return rpcc; }

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

	basic_network* _net;
	host_addr _addr;
	bool _bcast;

	friend class host_group;
	friend class basic_network;
	channel_set _incoming;
public:

	host(basic_network* n);
	virtual ~host();
	
	/**
		The network this host belongs to
	  */
	inline basic_network* net() const { return _net; }

	/**
		True this is a host group
	  */
	inline bool is_bcast() const { return _bcast; }

	/**
		Return the address of a host.

		If the host has not been assigned an address, this
		method will first call `set_addr()` without arguments,
		to set a default address.

		Note: if `set_addr()` has still not assigned a legal address,
		this call will
	  */
	host_addr addr();

	/**
		Ask for an address explicitly.
		If the address is successfully obtained, returns true,
		else it returns false and the address is left undefined.
	  */
	virtual bool set_addr(host_addr _a);

	/**
		Ask for a default address.

		Subclasses can override this method to define their own
		address policy. The default implementation assigns addresses
		as described in @ref host_addr.

		@see host_addr
	*/
	virtual void set_addr();

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
	/** Constructor  */
	host_group(basic_network* nw);

	/**
		Add a simple host to this group.
	  */
	void join(host* h);

	/**
		The members of the group
	  */
	const host_set& members() const { return grp; }
};



/**
	An rpc descriptor object.

	This is a base class for interfaces and methods.
  */
struct rpc_obj
{
	rpcc_t rpcc;
	rpc_obj() : rpcc(0) {}
	rpc_obj(rpcc_t _c) : rpcc(_c) {}
};

/**
	Represents a method in an rpc protocol
  */
struct rpc_method : rpc_obj, named
{
	bool one_way;
	rpc_method() {}
	rpc_method(rpcc_t _c, const string& _n, bool _o)
	: rpc_obj(_c), named(_n), one_way(_o) {}

	/**
		Return the number of channels (either 1 or 2) for this
		method.
	  */
	inline size_t num_channels() const { return one_way? 1 : 2; }
};

/**
	Represents an interface in an rpc protocol
  */
struct rpc_interface : rpc_obj, named
{
	vector<rpc_method> methods;
	unordered_map<string, size_t> name_map;


	rpc_interface();
	rpc_interface(rpcc_t _c, const string& _n); 

	/**
		Declare a method in the interface.
	  */
	rpcc_t declare(const string& mname, bool onew);

	/**
		Return a method object of the given rpcc.

		If there is no such method, an `std::invalid_argument`
		exception is raised.
	  */
	const rpc_method& get_method(rpcc_t rpcc) const;

	/**
		Return the number of channels that an instance of this
		interface creates.
	  */
	const size_t num_channels() const;

	/**
		Return the rpcc code for the method of the given name.

		If the method does not exist, 0 (an illegal rpcc) is returned.
	  */
	rpcc_t code(const string& name) const;
};

/**
	A collection of rpc interfaces.
  */
struct rpc_protocol
{
	vector<rpc_interface> ifaces;
	unordered_map<string, size_t> name_map;

	/**
		Declare an interface.
	  */
	rpcc_t declare(const string& name);

	/**
		Declare a method in some interface.

		This is just a shortcut for 
		```
		get_intercafe(ifc).declare(mname, onew)
		```
	  */
	rpcc_t declare(rpcc_t ifc, const string& mname, bool onew);

	const rpc_interface& get_interface(rpcc_t rpcc) const;
	const rpc_method& get_method(rpcc_t rpcc) const;

	rpcc_t code(const string& name) const;
	rpcc_t code(const type_info& ti) const;
	rpcc_t code(const string& name, const string& mname) const;
	rpcc_t code(const type_info& ti, const string& mname) const;


	// this is useful for chan_frame
	static const rpc_protocol empty;
};





struct rpc_call;

/**
	An rpc proxy represents a proxy object for some host.

	We do not want to pollute the class namespace, in order
	to avoid collisions with method names. Therefore, all
	member names start with `_r_`.
  */
struct rpc_proxy
{
	rpcc_t _r_ifc;
	vector<rpc_call*> _r_calls;
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
	rpcc_t _endpoint;
	channel* _req_chan = nullptr;
	channel* _resp_chan = nullptr;
	bool one_way;
public:
	rpc_call(rpc_proxy* _prx, bool _oneway, const string& _name);

	void connect(host* dst);

	inline rpcc_t endpoint() const { return _endpoint; }
	inline channel* request_channel() const { return _req_chan; }
	inline channel* response_channel() const { return _resp_chan; }
};


/**
	A collection of hosts and channels.

	This class manages the network elements: hosts, groups,
	channels, rpc endpoints.

	Rpc endpoints correspond to the concept of "message type".
	They are 32-bit entities, with the following form
	struct {
		unsigned ifc:   32-(n+1);
		unsigned rmeth:        n;
		unsigned resp:         1;
	}
	Roughly ifc is an "rmi interface code". Each host can 
	serve one or more of these.
	Each rmi interface has methods, and each method has a request
	and (if not `oneway`) a response.

	Currently, n is set to 7, that is, an interface can have up to
	127 methods (0 is not a legal rmeth value).

	Each interface and each method also have a string name, managed
	by this class. If the templated RPC facilities of this file are
	used, these strings are generated from the C++ classes without
	any user-code overhead.
  */
class basic_network : public named
{
protected:
	host_set _hosts;		// all the simple hosts
	host_set _groups;		// all the host groups
	channel_set _channels;	// all the channels

	// address maps
	std::unordered_map<host_addr, host*> addr_map;
	host_addr new_host_addr;
	host_addr new_group_addr;


	// rpc protocol
	rpc_protocol rpctab;

	friend class host;
public:

	basic_network();
	virtual ~basic_network();

	/// standard group, every host is added to it
	host_group all_hosts;

	/// The set of hosts
	inline const host_set& hosts() const { return _hosts; }

	/// The set of groups
	inline const host_set& groups() const { return _groups; }

	/// The set of channels
	inline const channel_set& channels() const { return _channels; }

	/// The number or hosts
	inline size_t size() const { return _hosts.size(); }

	/**
		Declare an interface by name.

		If the name has not been declared before, a new interface 
		is created for this name. Else, the old code is returned.
	 */
	rpcc_t decl_interface(const string& name);

	/**
		Declare an interface for a type_info object.

		The interface is registered to the type name for the
		type_info object, demangled into a human readable form.

		If the name has not been declared before, a new interface 
		is created for this name. Else, the old code is returned.
	  */
	rpcc_t decl_interface(const std::type_info& ti);

	/**
		Declare an interface for a type_index object.

		The interface is registered to the type name for the
		type_index object, demangled into a human readable form.

		If the name has not been declared before, a new interface 
		is created for this name. Else, the old code is returned.
	  */
	rpcc_t decl_interface(const std::type_index& tix);

	/**
		Declare an interface method by name.

		If a method by this name was not registered before
		name, a new one is registered and returned.
	 */ 
	rpcc_t decl_method(rpcc_t ifc, const string&, bool onew);


	/**
		Returns the RPC table
	  */
	const rpc_protocol& rpc() const { return rpctab; }

	/**
		Create a new RPC channel.

		@param src the source host
		@param dst the destination host
		@param rpcc the endpoint code
		@param dir the direction
	 */
	channel* connect(host* src, host* dest, rpcc_t rpcc);

	/**
		Assign an address to a host. 

		If `a` is a specific address, the call will assign `a` if available, else
		it will return `false`.

		If `a` is unknown_addr, then the call will assign a legal address and return
		`true`.
	  */
	bool assign_address(host* h, host_addr a);


	/**
		Reserve all addresses in the range between 0 and a.

		If `a` is non-negative, this call reserves addresses for hosts.
		If `a` is negative, this call reserves addresses for groups.

		These adresses are 'reserved' in the sense that they will not be 
		automatically assigned to a host. They are still available after
		this call, if they where available before the call
	  */
	void reserve_addresses(host_addr a);


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
	{
		set_addr(_sid);
	}

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
	set<source_id> hids;
	Hub* hub;
	unordered_map<source_id, Site*> sites;

	star_network(const set<source_id>& _hids) 
	: hids(_hids), hub(nullptr) 
	{ 
		// reserve the source_id addresses for the sites
		if(~ hids.empty()) {
			reserve_addresses(* hids.rbegin());
		}
	}

	template <typename ... Args>
	Net* setup(Args...args)
	{
		// create the nodes
		hub = new Hub((Net*)this, args...);

		for(auto hid : hids) 
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
	Marker name for remote methods.

	Writing 
	```
	oneway my_remote();
	```
	is preferable, from a documentation point of view, to 
	```
	void my_remote();
	```
  */
typedef void oneway;


struct _NAK {};
constexpr _NAK NAK;

/**
	Return type for remote methods that may optionally return a message.

	A remote function may return an ACK message (possibly with
	some payload), or just return NAK, in which case the middleware
	will not charge a response message at all.

	The payload must be default constructible and copyable/movable.
	For example:
	```
	Acknowledge<int> my_remote_method() {
		if(...)
			return 100;  // return a response
		else
			return NAK;  // do not acknowledge
	}
	```

	On the calling side:
	```
	auto ackint = proxy.my_remote_method();
	if(ackint)
		// use ackint.payload ...
	else
		// Call not acknowledged
	```
  */
template <typename Payload>
struct Acknowledge
{
	bool is_ack;
	Payload payload;

	Acknowledge(Payload&& p) : is_ack(true), payload(p) {}
	Acknowledge(const Payload& p) : is_ack(true), payload(p) {}
	Acknowledge(_NAK _nak) : is_ack(false), payload() {}

	inline operator bool() const { return is_ack; }

	inline size_t byte_size() const { 
		return is_ack ? dds::byte_size(payload) : 0;
	}
};

/**
	Acknowledgement response without payload
  */
template <>
struct Acknowledge<void>
{
	bool is_ack;
	constexpr Acknowledge(bool _is_ack) : is_ack(_is_ack) {}
	constexpr Acknowledge(_NAK _nak) : is_ack(false) {}

	inline operator bool() const { return is_ack; }
};

/// A handy short name 
typedef Acknowledge<void> Ack;

/// A convenience constant
constexpr Ack ACK(true);

// An ACK message has a 0-byte payload */
template <>
inline size_t byte_size< Ack >(const Ack& s) { return 0; }


template <typename T>
inline bool __transmit_response(const T& val) { return true; }

template <typename T>
inline bool __transmit_response(const Acknowledge<T>& ackval) {
	return ackval;
}


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
	: rpc_proxy(owner->net()->decl_interface(typeid(Process)), owner)
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



template <typename Dest, typename Response, typename ... Args>
struct remote_method;



template <typename Dest, typename Response, typename ... Args>
struct remote_method : proxy_method<Dest>
{
	typedef	Response (Dest::* method_type)(Args...);
	method_type method;

	remote_method(remote_proxy<Dest>* _proxy, method_type _meth, const string& _name)
	: proxy_method<Dest>(_proxy, false, _name), 
		method(_meth) 
	{ }

	inline Response operator()(Args...args) const
	{
		this->transmit_request(message_size(args...));
		Response r = (this->proxy->proc()->* (this->method))(
			std::forward<Args>(args)...
			);
		if( __transmit_response(r) )
			this->transmit_response(message_size(r));
		return r;
	}
};


template <typename Dest, typename ... Args>
struct remote_method<Dest, void, Args...> : proxy_method<Dest>
{
	typedef	void (Dest::* method_type)(Args...);
	method_type method;

	remote_method(remote_proxy<Dest>* _proxy, method_type _meth, const string& _name)
	: proxy_method<Dest>(_proxy, true, _name), method(_meth) 
	{ }

	inline void operator()(Args...args) const
	{
		this->transmit_request(message_size(args...));
		(this->proxy->proc()->* (this->method))
			(
			std::forward<Args>(args)...
			);
	}
};


template <typename T, typename Response, typename...Args>
inline remote_method<T, Response, Args...> 
make_remote_method(
	remote_proxy<T>* owner, 
	Response (T::*method)(Args...), 
	const string& _name
	)
{
	return remote_method<T, Response, Args...>(owner, method, _name);
}

#define REMOTE_METHOD(RClass, RMethod)\
 decltype(dds::make_remote_method((remote_proxy<RClass>*)nullptr,\
 	&RClass::RMethod, #RMethod )) RMethod  \
 { this, &RClass::RMethod, #RMethod }



/*	----------------------------------------

	Message utilities

	--------------------------------------- */


/**
	Subclasses of context can pass as RPC method arguments free of 
	cost.

	Theoretically, the arguments passed in this way are included
	in the middleware cost of sending a message. Therefore, they will
	be accounted by whatever cost model the user assigns to the
	network statistics, and in particular to the number of messages.

	Examples include: 
	- the sender host
	- a message timestamp or serial no
 */
struct call_context {
	size_t byte_size() const { return 0; }
};


/**
	Passing a pointer to the sender of a message as context.

	To use this, declare a remote method as
	```
	ret_type  my_remote(sender<sender_host> ctx, ...);
	```
  */
template <typename T>
struct sender : public call_context
{
	T* const value;
	sender(T* _h) : value(_h) {}
};



/**
	Wraps a pointer to the message.

	When a message object of type `T` is too big to pass by copy, declrare
	the remote method to accept `msgwrapper<T>`. Call the remote method
	using `wrap(&msg)`. The message size will be computed as
	`byte_size((const T&) *(&msg))`.
  */
template <typename T>
struct msgwrapper
{
	T* payload;
	msgwrapper(T* _p) : payload(_p){}
	inline size_t byte_size() const { return byte_size((const T&) *payload); }
};

/**
	Return a msgwrapper on the argument.
  */
template <typename T>
msgwrapper<T> wrap(T* p) { return msgwrapper<T>(p); }

/**
	Return a msgwrapper on the argument.
  */
template <typename T>
msgwrapper<T> wrap(T& p) { return msgwrapper<T>(&p); }



/*	----------------------------------------

	Statistics utilities

	--------------------------------------- */

/**
 	A fluent query interface over sets of channels
  */
struct chan_frame : vector<channel*>
{
	typedef vector<channel*> container;

	chan_frame() {}
	chan_frame(channel* c) : container{ c } { }
	chan_frame(const channel_set& cs) 
	: container(cs.begin(), cs.end()) { }

	chan_frame(const basic_network& nw) : chan_frame(nw.channels()) {}
	chan_frame(const basic_network* nw) : chan_frame(nw->channels()) {}

	inline const rpc_protocol& rpc() const {
		if(! empty())
			return front()->source()->net()->rpc();
		else
			return rpc_protocol::empty;
	}

	inline size_t count() const {
		size_t ret=0;
		for(auto c : *this) ret += c->messages();
		return ret;
	}

	inline size_t sum() const {
		size_t ret=0;
		for(auto c:*this) ret += c->bytes();
		return ret;
	}

	template <typename Pred>
	inline chan_frame select(const Pred& pred) const& {
		chan_frame cf;
		std::copy_if(begin(), end(), back_inserter(cf), pred);
		return cf;		
	}

	chan_frame src(host* _src) const {
		return select([&](channel *c) {
			return c->source() == _src;
		});
	}
	chan_frame src_in(const host_set& hs) const {
		return select([&](channel *c) {
			return hs.find(c->source())!=hs.end();
		});
	}

	chan_frame dst(host* _src) const {
		return select([&](channel *c) {
			return c->destination() == _src;
		});
	}
	chan_frame dst_in(const host_set& hs) const {
		return select([&](channel *c) {
			return hs.find(c->destination())!=hs.end();
		});
	}

	chan_frame endp(rpcc_t code, rpcc_t mask) const {
		return select([&](channel *c) {
			return (c->rpc_code()&mask) == (code&mask);
		});
	}
	chan_frame endp(const type_info& ti) const {
		return endp(rpc().code(ti), RPCC_IFC_MASK);
	}
	chan_frame endp(const string& ifname) const {
		return endp(rpc().code(ifname), RPCC_IFC_MASK);
	}

	chan_frame endp(const type_info& ti, const string& mname) const {
		return endp(rpc().code(ti, mname), RPCC_IFC_MASK);
	}
	chan_frame endp(const string& ifname, const string& mname) const {
		return endp(rpc().code(ifname, mname), RPCC_IFC_MASK);
	}

	chan_frame endp_req() const {
		return endp(0, RPCC_RESP_MASK);
	}
	chan_frame endp_rsp() const {
		return endp(1, RPCC_RESP_MASK);
	}

};






} // end namespace dss



#endif
