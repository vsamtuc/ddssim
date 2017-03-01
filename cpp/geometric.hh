#ifndef __GEOMETRIC_HH__
#define __GEOMETRIC_HH__

#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <cmath>
#include <vector>

#include "dds.hh"
#include "dsarch.hh"
#include "agms.hh"
#include "safezone.hh"
#include "method.hh"

using std::cout;
using std::endl;
using namespace agms;

namespace dds {  namespace gm {


/**
	Geometric method declarations are formed as collections of 
	components. Components come in families:
	- Data components, and
	- Protocol components
	- Lifecycle components

	Local site
	-----------

	The data components perform processing on the local state of  the streams. They are:

	- The local stream state (per stream_id)
	- The local statistic. This may or may not be the same as the local stream state
	  (e.g., in GEM it is not)
	- The safe zone component

	Protocol components are responsible for managing the execution of the algorithm.
	They are (ideally) data-agnostic.

	- The local violation component

	Lifecycle components control the lifecycle of other components
	- The node lifecycle

	Coordinator
	-----------

	Data components
*/


/*******************************************
	Concept: Local stream state
	Requires: 
		-
	Provides:
		<LocalStreamState> local_stream_state(stream_id)
		void local_stream_state_initialize()
		void update_stream_state(const dds_record&)
 *******************************************/


/*******************************************
	Concept: Local stream state sketch
	Extends: Local stream state
	Requires: 
		projection proj
 *******************************************/
template <typename C>
struct local_stream_state_sketch
{
	C& Self = static_cast<C&>(*this);

	map<stream_id, std::unique_ptr<isketch> > __local_stream_state;

	inline isketch& local_stream_state(stream_id sid) {
		return *(__local_stream_state[sid]);
	}

	inline void local_stream_state_initialize() {
		for(auto sid : CTX.metadata().stream_ids())
			__local_stream_state.emplace(sid, new isketch(Self.proj));
	}

	void update_stream_state(const dds_record& rec) {
		assert(rec.hid == Self.site_id());
		local_stream_state[rec.sid].update(rec.key, (rec.sop==stream_op::INSERT)?1.0:-1.0);
	}
};


/*******************************************
	Concept: Local drift vector
	Requires:
		 local_stream_state(stream_id)
	Provides:
		<DriftVector> drift_vector(stream_id)
		void update_drift_vector(stream_id)
		void drift_vector_initialize()
 *******************************************/

template <typename C>
struct local_drift_vector_is_state
{
	C& Self = static_cast<C&>(*this);
	inline auto& drift_vector(stream_id sid) { return Self.local_stream_state(sid); }
	inline void drift_vector_initialize() { }
	inline void update_drift_vector(stream_id sid) { }
};


/*******************************************
	Concept: Safe zone
	Requires:
		
	Provides:
		<DriftVector> drift_vector(stream_id)
		void update_drift_vector(stream_id)
		void drift_vector_initialize()
 *******************************************/
	



/*******************************************
	Node lifecycle

	Requires:
		void local_stream_state_initialize()
	Provides:
		void node_initialize()
 *******************************************/
template <typename C>
struct node_lifecycle_mixin
{
	C& Self = static_cast<C&>(*this);
	void node_initialize() {
		Self.local_stream_state_initialize();
		Self.drift_vector_initialize();
	}
};




///////////////////////////////////////////////////////
//
// Network definition
//
///////////////////////////////////////////////////////



struct network;

struct coordinator : process
{
	coordinator(network* _net, const projection& _proj);
};


struct node : 
	local_site, 
	local_stream_state_sketch<node>,
	local_drift_vector_is_state<node>,
	node_lifecycle_mixin<node>
{
	projection proj;

	node(network*, source_id, const projection&);
};



struct network : star_network<network, coordinator, node>
{
	network(projection _proj)
	: star_network<network, coordinator, node>(CTX.metadata().source_ids())
	{
		setup(_proj);
	}
};


inline coordinator::coordinator(network* _net, const projection& _proj) 
: process(_net) {}

inline node::node(network* _net, source_id _sid, const projection& _proj) 
: local_site(_net, _sid), proj(_proj)
{  
	node_initialize();
}


} }  // end namespace dds::gm


/***************************************************************************************
 ***************************************************************************************
 ***************************************************************************************
 ***************************************************************************************
 	
 		Reference impl of the new method

 ***************************************************************************************
 ***************************************************************************************
 ***************************************************************************************
 ***************************************************************************************
 ***************************************************************************************/

namespace dds { namespace gm2 {




/**
	This class wraps safezone functions. It serves as part of the protocol.

	Semantics: if `valid()` returns false, the system is in "promiscuous mode"
	i.e., every update is forwarded to the coordinator immediately.

	Else, there are two options: if a valid safezone function is given, then
	it is used. Else, the naive function is used:
	\f[  \zeta(X) = \zeta_{E} - \| U \|  \f]
	where \f$ u \f$ is the current drift vector.
  */
struct safezone
{
	selfjoin_agms_safezone* szone; // the safezone function, if any
	selfjoin_agms_safezone::incremental_state incstate; // cached here for convenience
	double incstate_naive;

	sketch* Eglobal;		// implementation detail, also used to compute the byte size
	size_t updates;			// number of updates, determines the size of the global sketch
	double zeta_E;			// This is acquired by calling the safezone 

	// invalid
	safezone() : szone(nullptr), Eglobal(nullptr), updates(0), zeta_E(-1) {};

	// valid safezone
	safezone(selfjoin_agms_safezone* sz, sketch* E, size_t upd, double zE)
	: szone(sz), Eglobal(E), updates(upd), zeta_E(zE)
	{
		assert(sz && sz->isvalid);
		assert(zE>0);
	}

	// must be valid, i.e. zE>0
	safezone(double zE) 
	:  szone(nullptr), Eglobal(nullptr), updates(0), zeta_E(zE)
	{
		assert(zE>0);
	}

	// We take these to be non-movable!
	safezone(safezone&&)=delete;
	safezone& operator=(safezone&&)=delete;

	// Copyable
	safezone& operator=(const safezone&) = default;

	inline bool naive() const { return szone==nullptr && zeta_E>0; }
	inline bool full() const { return szone!=nullptr; }
	inline bool valid() const { return full() || naive(); }

	double prepare_inc(const isketch& U)
	{
		if(full()) {
			sketch X = (*Eglobal)+U;
			return szone->with_inc(incstate, X);
		} else if(naive()) {
			return zeta_E - norm_L2_with_inc(incstate_naive, U);
		} else {
			assert(!valid());
			return NAN;
		}
	}

	double operator()(const isketch& U)
	{
		if(full()) {
			delta_vector DX = U.delta;
			DX += *Eglobal;
			return szone->inc(incstate, DX);
		}
		else if(naive()) {
			return zeta_E - norm_L2_inc(incstate_naive, U.delta);
		} else {
			assert(! valid());
			return NAN;			
		}
	}

	size_t byte_size() const {
		if(!valid()) 
			return 1;
		else if(szone==nullptr)
			return sizeof(zeta_E);
		else
			return compressed_sketch{*Eglobal, updates}.byte_size();
	}
};



struct coordinator;
struct node;

struct network : star_network<network, coordinator, node>, reactive
{
	stream_id sid;
	projection proj;
	double beta;

	network(stream_id, const projection& _proj, double _beta);

	network(stream_id _sid, depth_type D, index_type L, double _beta) 
	: network(_sid, projection(D,L), _beta) 
	{
		
	}

	void process_record();
	void process_init();
	void output_results();
};


/**
	
 */
template <typename ProxyType, typename ProxiedType = typename ProxyType::proxied_type>
class proxy_map
{
public:
	typedef  ProxiedType  proxied_type;
	typedef ProxyType  proxy_type;

	process* owner;

	proxy_map() {}

	proxy_map(process* _owner) : owner(_owner) {  }

	proxy_type& operator[](proxied_type* proc) {
		return * pmap[proc];
	}

	void add(proxied_type* proc) {
		if(pmap.find(proc)!=pmap.end()) return;
		pmap[proc] = new proxy_type(owner);
		* pmap[proc] <<= (proc);
	}

	template <typename StarNet>
	void add_sites(const StarNet* net) {
		for(auto&& i : net->sites) 
			add(i.second);
	}

	auto begin() const { return pmap.begin(); }
	auto end() const { return pmap.end(); }

	size_t size() const { return pmap.size(); }

private:
	std::map<proxied_type*, proxy_type*> pmap;

};


struct node_proxy;

using std::vector;


struct coordinator : process
{
	proxy_map<node_proxy, node> proxy;

	//
	// protocol stuff
	//
	selfjoin_query query;	// current query state
	size_t total_updates;	// number of stream updates received

	bool in_naive_mode;		// when true, use the naive safezone

	size_t k;				// number of sites

	// index the nodes
	map<node*, size_t> node_index;
	vector<node*> node_ptr;

	// protocol related
	vector<bool> has_naive;
	vector<int> bitweight;
	vector<size_t> updates;
	vector<size_t> msgs;

	const double P = 0.5;
	int bit_budget;
	int bit_level;
	size_t round_updates;

	// report the series 
	computed<double> Qest_series;

	coordinator(network* nw, const projection& proj, double beta); 

	inline network* net() { return dynamic_cast<network*>(host::net()); }

	void setup_connections() override;

	// initialize a new round
	void start_round();
	void finish_round();

	// remote call on host violation
	oneway threshold_crossed(sender<node> ctx, int delta_bitw);
};




struct coord_proxy : remote_proxy<coordinator>
{
	REMOTE_METHOD(coordinator, threshold_crossed);
	coord_proxy(process* c) : remote_proxy<coordinator>(c) { }
};




struct node : local_site
{
	safezone szone;	// pointer to the safezone (shared among objects)

	double minzeta; 		// minimum value of zeta so far
	double zeta;			// current zeta

	double zeta_0;			// start value for discretization, equal to zeta at last reset_bitweight()
	double zeta_quantum;	// discretization for bitweight, set by reset_bitweight()
	int bitweight;			// equal to number of bits sent since last reset_bitweight()


	isketch U;				// drift vector
	size_t update_count;	// number of updates in drift vector

	coord_proxy coord;

	node(network* net, source_id hid, const projection& proj, double beta)
	: local_site(net, hid), 
		U(proj), update_count(0),
		coord( this )
	{ 
		coord <<= net->hub;
	}


	void update_stream();

	//
	// Remote methods
	//

	oneway reset(const safezone& newsz) { 
		// reset the safezone object
		(sketch&)U = 0.0;
		update_count = 0;

		szone = newsz;
		szone.prepare_inc(U);
		zeta = minzeta = szone.zeta_E;
		reset_bitweight(szone.zeta_E/2);
	}

	double get_zeta() {
		return zeta;
	}

	oneway reset_bitweight(double Z)
	{
		zeta_0 = zeta;
		zeta_quantum = Z;
		bitweight = 0;
	}

	compressed_sketch get_drift() {
		return compressed_sketch { U, update_count };
	}
};

struct node_proxy : remote_proxy<node>
{
	REMOTE_METHOD(node, reset);
	REMOTE_METHOD(node, reset_bitweight);
	REMOTE_METHOD(node, get_drift);
	REMOTE_METHOD(node, get_zeta);
	node_proxy(process* p) : remote_proxy<node>(p) {}
};

} // end namespace dds::gm


template <>
inline size_t byte_size<gm2::node*>(gm2::node  * const & ) { return 4; }


}  // end namespace dds




#endif
