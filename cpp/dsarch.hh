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
#include <map>

#include "dds.hh"

namespace dds {


class basic_network;
class host;
class host_group;
class process;
class channel;

/**
	RPC code type.

	An rpcc code is a 32-bit number, representing
	a uni-directional channel class (or set). These
	sets of channels are called endpoints.

	Each channel is a triple (src, dest, rpcc), where
	\c src is the source host, \c dst is the destination host
	and \c rpcc is the unique rpc code it is associated with.

	A remote method (i.e., remote function) can have
	two endpoints, if it is bidirectional, or only 
	one endpoint, if it is \c oneway. But see broadcast channels.

	The bits are encoded as follows:
	- The least significant bit corresponds to the
	  request channel (0) or response channel (1)
	- The \c RPCC_BITS_PER_IFC least significant bits
	  correspond to the methods of an interface.
	  There can be up to 127 such methods in the interface,
	  with up to 2 endpoints each.

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
	size (in bytes). A channel is defined by
	- the source host
	- the destination host
	- the type of message (rpcc code)

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

	Finally, a channel can be a multicast channel. This is always associated
	with some one-way rpc method, sendind data from a single source host A
	to a destination host group B. Again, there are two channels associated with
	A and B. One channel counts the traffic sent by A, and the second channel
	counts the traffic received by all the hosts in host group B. For example,
	if there are 3 hosts in group B, and a message of 100 bytes is sent, 
	one channel will register one additional message and 100 additional bytes,
	and the other will register 3 additional messages and 300 additional bytes.
	Note that the membership of a host group can change dynamically.

  */
class channel
{
protected:
	host *src, *dst;
	rpcc_t rpcc;

	size_t msgs, byts;

	channel(host *s, host* d, rpcc_t rpcc);
public:
	virtual ~channel();

	/** The source host */
	inline host* source() const { return src; }

	/** The destination host */
	inline host* destination() const { return dst; }

	/** The rpcc code */
	inline rpcc_t rpc_code() const { return rpcc; }

	/** Number of messages sent */
	inline auto messages() const { return msgs; }

	/** Number of bytes sent */
	inline auto bytes() const { return byts; }

	/** 
		Number of messages received. 
		For broadcast channels this is not the same as
		the number of messages sent.
	  */
	virtual size_t messages_received() const { return msgs; }

	/** 
		Number of bytes received. 
		For broadcast channels, this is not the same as the
		bytes sent.
	  */
	virtual size_t bytes_received() const { return byts; }

	/**
		Register the transmission of a message on this channel

		@param msg_size the number of bytes in the transmitted message.
	  */
	virtual void transmit(size_t msg_size);

	virtual string repr() const;

	friend class basic_network;
	friend class host;
};


class multicast_channel : public channel
{
protected:
	size_t rxmsgs, rxbyts;

	multicast_channel(host *s, host_group* d, rpcc_t rpcc);	
public:

	virtual size_t messages_received() const override;
	virtual size_t bytes_received() const override;
	virtual void transmit(size_t msg_size) override;

	virtual string repr() const override;

	friend class basic_network;
	friend class host;

};


typedef std::unordered_set<channel*> channel_set;
typedef std::unordered_set<host*> host_set;

/**
	Hosts are used as nodes in the basic_network.

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
	bool _mcast;

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
	inline bool is_mcast() const { return _mcast; }

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
	
	This is simply an abstract base class. The implementation
	of this class can be anything. All that this class interface
	provides is the methods that are required by the
	communication traffic computation.
  */
class host_group : public host
{
public:
	/** Constructor  */
	host_group(basic_network* nw);

	/**
		The members of the group
	  */
	virtual size_t receivers(host* sender)=0;
};


/**
	A typed implementation of \c host_group

	This implementation stores the group hosts in an \c std::set.
  */
template <typename Process>
struct mcast_group : host_group
{ 
	typedef set<Process*> Container;
private:
	Container memb;
public:
	typedef typename Container::iterator iterator;

	inline mcast_group(basic_network* _nw) : host_group(_nw) { }

	inline void join(Process* host) {  memb.insert(host); }
	inline void leave(Process* host) { memb.erase(host); }

	inline bool contains(Process* host) const { return memb.count(host)>0; }
	inline iterator begin() { return memb.begin(); }
	inline iterator end() { return memb.end(); }

	virtual size_t receivers(host* sender) override {
		auto snd = dynamic_cast<Process*>(sender);
		return memb.size() - memb.count(snd);
	}

};




/**
	An rpc descriptor object.

	This is a base class for interfaces and methods.
	It only holds the rpcc code for the interface or method.
  */
struct rpc_obj
{
	rpcc_t rpcc;
	rpc_obj() : rpcc(0) {}
	rpc_obj(rpcc_t _c) : rpcc(_c) {}
};

/**
	Represents a method in an rpc protocol.

	A remote method represents a method that hosts can call
	on other hosts. This type basically only holds the name
	of the remote method and whether it is one-way or not.
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
	Represents an interface in an rpc protocol. 

	An interface is like a 'remote type'. It represents
	a collection of remote functions that are implemented on
	a remote host.
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
	\brief A collection of rpc interfaces. 

	A protocol is the collection of RPC interfaces used in a network.
  */
struct rpc_protocol : public named
{
	rpc_protocol() {}
	rpc_protocol(const string& name) : named(name) {}

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

	When host A wants to call a remote method on host B, 
	it makes the call through an rpc proxy method, so that
	the network traffic can be accounted for. Host A is the
	owner of the proxy and host B is the proxied host (process).

	Each proxy is associated with an rpc interface, which
	represents the collection of remote calls (rpc functions)
	being proxied. In middleware terms, the proxy instantiates
	the interface.

	This class serves as the base class for the
	\c remote_proxy<T> template class.

	We do not want to pollute the class namespace, in order
	to avoid collisions with method names. Therefore, all
	member names start with `_r_`.
  */
class rpc_proxy
{
public:
	/** Interface code for the proxy. */
	rpcc_t _r_ifc;

	/** The collection of calls */
	vector<rpc_call*> _r_calls;

	/** The owner of the proxy is the process that holds the proxy */
	host* _r_owner;

	/** This is the node being proxied. */
	host* _r_proc = nullptr;
private:
	template <typename Dest>
	friend class remote_proxy;
	template <typename Dest>
	friend class multicast_proxy;
	friend class rpc_call;
	rpc_proxy(size_t ifc, host* _own);
	size_t _r_register(rpc_call* call);
	void _r_connect(host* dst);
public:
	rpc_proxy(const string& name, host* _own);
};


/**
	An rcp call  belongs to some specific rpc proxy.
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
	virtual ~rpc_call();

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
class basic_network : public virtual named
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


	/**
		A factory method for channels.

		Subclasses can overload this method, in order to supply custom channels to the
		network. Notice that the semantics of custom channels must follow the semantics
		of standard channels. In particular:
		- if 

		This method should not be confused with \c connect(). The \c connect() is the method
		that should be called to construct the network. This method is called internally
		by \c connect().
	  */
	virtual channel* create_channel(host* src, host* dest, rpcc_t rpcc) const;

public:

	/** A default constructor */
	basic_network();
	virtual ~basic_network();

	/// Standard group, every host is added to it, although
	/// not much use, except taking an address.
	/// Maybe in the future...
	struct all_hosts_group : host_group
	{
		all_hosts_group(basic_network* _nw) : host_group(_nw) {}
		size_t receivers(host* h) { return net()->_hosts.size()-1; }
	};
	all_hosts_group all_hosts;

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
		Set the protocol name
	  */
	void set_protocol_name(const string& pname) { rpctab.set_name(pname); }

	/**
		Create a new RPC channel.

		@param src the source host
		@param dst the destination host
		@param rpcc the endpoint code
		@param dir the direction
	 */
	channel* connect(host* src, host* dest, rpcc_t rpcc);

	/**
		Destroy an RPC channel.
	  */
	void disconnect(channel* c);


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

	/**
		Get a host by the address if it exists.

		If no host exists with this address, this function returns null.
	  */
	host* by_addr(host_addr a) const;
};


/**
	A process extends a host with remote methods.
  */
class process : public host
{
public:
	using host::host;

	/**
		This callback is called after network initialization.

		This callback exists so that
		the process can establish connections and perform overall
		configuration.

		Subclasses should override this method to customize behaviour.
		The default implementation does nothing.
	  */
	virtual void setup_connections() { }

	/**
		This is called for every node before network finalization.
	 */
	virtual void finalize() { }
};


/**
   A local site accepts the input from streams.

   This is a base class for classes implementing hosts that 
   process stream records.
   */
class local_site : public process
{
protected:
	source_id sid=-1;

public:
	local_site(basic_network* nw, source_id _sid) 
	: process(nw), sid(_sid) 
	{
		if(!set_addr(_sid))
			throw std::runtime_error("Local site could not acquire the hid address");
	}

	/**
	  Return the \c source_id of the stream accepted
	  */
	inline source_id site_id() const { return sid; }

	/**
	   The handler called when a new stream record is available.

	   Subclasses should override this method to customize behaviour.
	  */
	virtual void handle(const dds_record& rec) {}
};


/**
	A star network topology.

	In a star network, every regular node (site) is connected to 
	a central node (hub).

	Nodes in a star network are local sites.

	This class is a mixin. A concrete class \c mynetwork is defined
	as 
	\code[c++]
     class mynetwork 
       : public star_network<mynetwork, myhub, mysite>, // all 3 types are incomplete
       public reactive
     { ... };
	\endcode

	@tparam Net  the concrete network class composed by this mixin
	@tparam Hub  the node type for the hub node (aka. coordinator)
	@tparam Site the node type for local sites
  */
template <typename Net, typename Hub, typename Site>
struct star_network : public basic_network
{
	typedef Net network_type;
	typedef Hub hub_type;
	typedef Site site_type;

	set<source_id> hids;
	Hub* hub;
	vector<Site*> sites;

	star_network(const set<source_id>& _hids) 
	: hids(_hids), hub(nullptr) 
	{ 
		// reserve the source_id addresses for the sites
		if(! hids.empty()) {
			reserve_addresses(* hids.rbegin());
		}
	}

	inline site_type* source_site(source_id hid) const {
		return static_cast<site_type*>(by_addr(hid));
	}
 
	template <typename ... Args>
	Net* setup(Args...args)
	{
		// create the nodes
		hub = new Hub((Net*)this, args...);


		for(auto hid : hids) 
		{
			Site* n = new Site((Net*)this, hid, args...);
			n->set_addr(hid);
			sites.push_back(n);
		}

		// make the connections
		hub->setup_connections();
		for(auto n : sites) {
			n->setup_connections();
		}

		return (Net*)this;
	}

	~star_network() 
	{
		// Delete the nodes that we created...
		for(auto n : sites) {
			n->finalize();
		}
		hub->finalize();

		for(auto n : sites) {
			delete n;
		}
		delete hub;
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


/**
	Base class for implementing remote proxies.

	To define a proxy on class \c C, a user should define a sublclass
	\c class C_proxy : remote_proxy<C> 

	Each remote proxy is associated with two objects:
	- its _owner_ is an object of any class which is a subclass of host
	- its _destination_ is an object of a subclass of \c Process

	This is a typed subclass of \c rpc_proxy, with no attributes.

	@tparam Process the base class for proxied objects.
  */
template <typename Process>
class remote_proxy : public rpc_proxy
{
public:
	typedef Process proxied_type;

	/**
		Construt a proxy object for the given owner.
	  */
	inline remote_proxy(process* owner) 
	: rpc_proxy(owner->net()->decl_interface(typeid(Process)), owner)
	{ }

	/**
		Connects this proxy to a destination.
		This going to be a unicast proxy.
	  */
	inline void operator<<=(Process* dest) { _r_connect(dest); 	}

	/**
		Connects this proxy to a destination.
		This going to be a unicast proxy.
	  */
	inline void operator<<=(Process& dest) { _r_connect(&dest); }

	/**
		Connects this proxy to a destination.
		This going to be a multicast proxy.
	  */
	inline void operator<<=(mcast_group<Process>* dest) { _r_connect(dest); 	}

	/**
		Connects this proxy to a destination.
		This going to be a multicast proxy.
	  */
	inline void operator<<=(mcast_group<Process>& dest) { _r_connect(&dest); }

	/**
		The process proxied by this proxy.
	  */
	inline Process* proc() const { 
		return dynamic_cast<Process*>(_r_proc); 
	}

	inline mcast_group<Process>* proc_group() const {
		return dynamic_cast<mcast_group<Process>*>(_r_proc); 		
	}
};


/**
	This is a base class for \c remote_method<T,...>.

	This is a typed subclass of \c rpc_call, with no attributes.
  */
template <typename Dest>
struct proxy_method  : rpc_call
{
	typedef remote_proxy<Dest> proxy_type;
	
	inline proxy_type* proxy() const { return static_cast<proxy_type*>(_proxy); }

	inline proxy_method(proxy_type* _proxy, bool one_way, const string& _name) 
	: rpc_call(_proxy, one_way, _name) {}

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
		Dest* target = this->proxy()->proc();
		assert(target);
		this->transmit_request(message_size(args...));
		Response r = (target->* (this->method))(
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
		// Here we must distinguish the case of having a unicast or
		// multicast call

		// Try the unicast case first, as it is probably more common
		Dest* utarget = this->proxy()->proc();
		if(utarget!=nullptr) {
			// unicast case
			this->transmit_request(message_size(args...));
			(utarget->* (this->method))(	std::forward<Args>(args)...	);
		} else {
			mcast_group<Dest>* mtarget = this->proxy()->proc_group();
			assert(mtarget);
			this->transmit_request(message_size(args...));
			// issue the calls
			for(Dest* target : *mtarget) 			
				(target->* (this->method))(	std::forward<Args>(args)...	);
		}
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



/**
	A proxy map is a map container for mapping host objects to their proxies.

	Each site maintaining a set of proxies on a number of objects, can
	use a proxy map store pointers to its proxies.
 */
template <typename ProxyType, typename ProxiedType = typename ProxyType::proxied_type>
class proxy_map
{
public:
	typedef  ProxiedType  proxied_type;
	typedef ProxyType  proxy_type;

	/**
		Create an unowned empty proxy map
	  */
	proxy_map() : owner(nullptr) {}

	/**
		Create a proxy for the given owner
	  */
	proxy_map(process* _owner) : owner(_owner) {  }

	/**
		Destroy proxy map and all proxies it created
	  */
	~proxy_map() {
		// remove the proxies 
		for(auto m : pmap)
			delete m.second;
		for(auto m : mpmap)
			delete m.second;
	}

	/**
		Get the proxy for the given process.
	  */
	proxy_type& operator[](proxied_type* proc) { return * add(proc); }
	proxy_type& operator[](proxied_type& proc) { return * add(&proc); }

	/**
		Get the proxy for a given process group.
	  */
	proxy_type& operator[](mcast_group<proxied_type>* mproc) { return * add(mproc); }
	proxy_type& operator[](mcast_group<proxied_type>& mproc) { return * add(&mproc); }

	/**
		Add a process to the proxy map, creating its proxy.
	  */
	proxy_type* add(proxied_type* proc) {
		if(owner==nullptr)
			throw std::runtime_error("Proxy map has not been owned yet.");
		auto it =  pmap.find(proc);
		if(it!=pmap.end()) return it->second;
		proxy_type* prx = new proxy_type(owner);
		*prx <<= proc;
		pmap[proc] = prx;
		return prx;
	}

	/** 
		Add a mcast_group to the map.
	  */
	proxy_type* add(mcast_group<proxied_type>* mproc) {
		if(owner==nullptr)
			throw std::runtime_error("Proxy map has not been owned yet.");
		auto it =  mpmap.find(mproc);
		if(it!=mpmap.end()) return it->second;
		proxy_type* prx = new proxy_type(owner);
		*prx <<= mproc;
		mpmap[mproc] = prx;
		return prx;
	}

	/**
		Add proxies to all sites in a container of sites.

		It the owner is a member of the container, it will be
		excluded.
	  */
	template <typename SiteContainer>
	void add_sites(const SiteContainer& sites) {
		for(auto&& h : sites) 
			if(h != owner) add(h);
	}


private:
	process* owner;
	std::map<proxied_type*, proxy_type*> pmap;
	std::map<mcast_group<proxied_type>*, proxy_type*> mpmap;
};



/*	----------------------------------------

	Statistics utilities

	--------------------------------------- */

/**
 	A fluent query interface over sets of channels.

 	This can be used to rapidly select a particular set of
 	channels from the network. It is a rather slow implementation,
 	but it is quite adequate for statistics that will only be computed
 	at the end of an experiment.
  */
struct chan_frame : vector<channel*>
{
	typedef vector<channel*> container;

	chan_frame() {}

	// single channel
	chan_frame(channel* c) : container{ c } { }

	// channel set
	chan_frame(const channel_set& cs) 
	: container(cs.begin(), cs.end()) { }

	// Constructor from network
	chan_frame(const basic_network& nw) : chan_frame(nw.channels()) {}
	chan_frame(const basic_network* nw) : chan_frame(nw->channels()) {}

	// The protocol of the network
	inline const rpc_protocol& rpc() const {
		if(! empty())
			return front()->source()->net()->rpc();
		else
			return rpc_protocol::empty;
	}

	//
	// Statistics. Only basic tallies are supported, but it is
	// trivial to use a frame with more complex queries, in code.
	//

	// total messages over all channels
	inline size_t msgs() const {
		size_t ret=0;
		for(auto c : *this) ret += c->messages();
		return ret;
	}

	// total bytes over all channels
	inline size_t bytes() const {
		size_t ret=0;
		for(auto c:*this) ret += c->bytes();
		return ret;
	}


	// total received messages over broadcast channels
	inline size_t recv_msgs() const {
		size_t ret=0;
		for(auto c : *this) {
			multicast_channel* bc = dynamic_cast<multicast_channel*>(c);
			if(bc!=nullptr)
				ret += c->messages_received();
		}
		return ret;
	}


	// total received bytes over broadcast channels
	inline size_t recv_bytes() const {
		size_t ret=0;
		for(auto c : *this) {
			multicast_channel* bc = dynamic_cast<multicast_channel*>(c);
			if(bc!=nullptr)
				ret += c->bytes_received();
		}
		return ret;
	}


	//
	//
	//  Selection facilities
	//
	//

	template <typename Pred>
	inline chan_frame select(const Pred& pred) const& {
		chan_frame cf;
		std::copy_if(begin(), end(), back_inserter(cf), pred);
		return cf;		
	}

	//
	// Filter by source / destination
	//

	// Filter by source
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

	// Filter by destination
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

	// Filter only unicast/multicast channels
	chan_frame unicast() const {
		return select([&](channel *c) {
			return ! c->destination()->is_mcast();
		});		
	}
	chan_frame multicast() const {
		return select([&](channel *c) {
			return c->destination()->is_mcast();
		});		
	}

	// 
	// filter by endpoint
	//

	chan_frame endp(rpcc_t code, rpcc_t mask) const {
		return select([&](channel *c) {
			return (c->rpc_code()&mask) == (code&mask);
		});
	}

	// By interface (rpc_interface)
	chan_frame endp(const type_info& ti) const {
		return endp(rpc().code(ti), RPCC_IFC_MASK);
	}
	chan_frame endp(const string& ifname) const {
		return endp(rpc().code(ifname), RPCC_IFC_MASK);
	}

	// By remote method (rpc_method)
	chan_frame endp(const type_info& ti, const string& mname) const {
		return endp(rpc().code(ti, mname), RPCC_IFC_MASK);
	}
	chan_frame endp(const string& ifname, const string& mname) const {
		return endp(rpc().code(ifname, mname), RPCC_IFC_MASK);
	}

	// By endpoint direction
	chan_frame endp_req() const {
		return endp(0, RPCC_RESP_MASK);
	}
	chan_frame endp_rsp() const {
		return endp(1, RPCC_RESP_MASK);
	}

	//
	// Union, negation
	//
	chan_frame union_with(const chan_frame& other) const {
		channel_set u;
		u.insert(begin(), end());
		u.insert(other.begin(), other.end());
		return chan_frame(u);
	}

	chan_frame except(const chan_frame& other) const {
		channel_set u(begin(), end());
		for(auto&& c : other) u.erase(c);
		return chan_frame(u);
	}

};




} // end namespace dss



#endif
