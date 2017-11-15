#ifndef __SGM_HH__
#define __SGM_HH__

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
#include "gm.hh"

using std::cout;
using std::endl;


namespace gm { namespace sgm {

using namespace dds;
using namespace agms;
using std::vector;

template <qtype QType>
struct coordinator;

template <qtype QType>
struct node;



template <qtype QType>
struct network 
	: star_network<network<QType>, coordinator<QType>, node<QType> >, 
	component
{
	typedef coordinator<QType> coordinator_t;
	typedef node<QType> node_t;
	typedef network<QType> network_t;
	typedef star_network<network_t, coordinator_t, node_t> star_network_t;

	stream_id sid;
	projection proj;
	double beta;

	network(const string& _name, stream_id, const projection& _proj, double _beta);

	network(const string& _name, stream_id _sid, depth_type D, index_type L, double _beta) 
	: network(_name, _sid, projection(D,L), _beta) 
	{
		
	}

	void process_record();
	void process_init();
	void output_results();
};




template <qtype QType>
struct node_proxy;



template <qtype QType>
struct coordinator : process
{
	typedef coordinator<QType> coordinator_t;
	typedef node<QType> node_t;
	typedef node_proxy<QType> node_proxy_t;
	typedef network<QType> network_t;

	proxy_map<node_proxy_t, node_t> proxy;
	typedef tuple<node_proxy_t*,double>  node_double;

	//
	// protocol stuff
	//
	selfjoin_query query;	// current query state
	size_t total_updates;	// number of stream updates received

	bool in_naive_mode;		// when true, use the naive safezone

	size_t k;				// number of sites

	// index the nodes
	map<node_t*, size_t> node_index;
	vector<node_t*> node_ptr;

	// protocol related
	vector<bool> has_naive;

	// report the series 
	computed<double> Qest_series;


	coordinator(network_t* nw, const projection& proj, double beta); 
	~coordinator();

	inline network_t* net() { return static_cast<network_t*>(host::net()); }

	void setup_connections() override;

	// load the warmup dataset
	void warmup();

	// initialize a new round
	void start_round();
	void finish_round();
	
	// initialize a new subround
	void start_subround(double total_zeta);
	void finish_subrounds(double total_zeta);

	//
	// rebalancing
	//

	// used during rebalancing

	set<node_t*> B;			// initialized by local_violation(), 
								// updated by rebalancing algo

	set<node_t*> Bcompl;	// complement of B, updated by rebalancing algo

	sketch Ubal;				// contains \sum_{i\in B} U_i
	size_t Ubal_updates;		// Ubal updates

	bool Ubal_admissible;		// contains zeta(Ubal)>0

	size_t round_total_B;           // total size of B over round
  

	// This method performs the actual rebalancing and returns the delta_zeta
	// of the rebalanced nodes.
	void rebalance();

	// Returns a rebalancing set two nodes, or empty.
	void rebalance_random(node_t* lvnode);
	void rebalance_random_limits(node_t* lvnode);
	
	//
	// this is used to trace the execution of rounds, for debugging or tuning
	//
	void trace_round(sketch& newE);

	// remote call on host violation
	oneway local_violation(sender<node_t> ctx);

	// statistics
	size_t num_rounds;				 // total number of rounds
	size_t num_subrounds;			 // total number of subrounds
	size_t sz_sent;					 // total safe zones sent
	size_t total_rbl_size; 			 // sum of all rebalance sets
};




template <qtype QType>
struct coord_proxy : remote_proxy< coordinator<QType> >
{
	using coordinator_t = coordinator<QType>;
	REMOTE_METHOD(coordinator_t, local_violation);
	coord_proxy(process* c) : remote_proxy<coordinator_t>(c) { }
};


/**
	This is a site implementation for the classic Geometric Method protocol.

 */
template <qtype QType>
struct node : local_site
{
	typedef coordinator<QType> coordinator_t;
	typedef node<QType> node_t;
	typedef node_proxy<QType> node_proxy_t;
	typedef network<QType> network_t;
	typedef coord_proxy<QType> coord_proxy_t;

	int num_sites;				// number of sites

	safezone szone;	// pointer to the safezone (shared among objects)

	double zeta_l, zeta_u;          // zetas of lower and upper bounds
	double zeta;			// current zeta = min(zeta_l, zeta_u)

	isketch U;				// drift vector
	size_t update_count;	// number of updates in drift vector

	size_t round_local_updates; // number of local stream updates since last reset

	coord_proxy_t coord;

	node(network_t* net, source_id hid, const projection& proj, double beta)
	: local_site(net, hid), 
		U(proj), update_count(0),
		coord( this )
	{ 
		coord <<= net->hub;
	}

	void setup_connections() override;

	void update_stream();

	//
	// Remote methods
	//

	oneway reset(const safezone& newsz) { 
		// reset the safezone object
		szone = newsz;

		// reset the drift vector
		(sketch&)U = 0.0;
		update_count = 0;
		zeta = szone.prepare_inc(U, zeta_l, zeta_u);
		assert(zeta==szone.zeta_E);

		// reset round statistics
		round_local_updates = 0;
	}


	compressed_sketch get_drift() {
		return compressed_sketch { U, update_count };
	}

	void set_drift(compressed_sketch newU) {
		(sketch&)U = newU.sk;
		update_count = newU.updates;
		zeta = szone.prepare_inc(U, zeta_l, zeta_u);
		assert(zeta>0);
	}


};

template <qtype QType>
struct node_proxy : remote_proxy< node<QType> >
{
	typedef node<QType> node_t;
	REMOTE_METHOD(node_t, reset);
	REMOTE_METHOD(node_t, get_drift);
	REMOTE_METHOD(node_t, set_drift);
	node_proxy(process* p) : remote_proxy<node_t>(p) {}
};

} // end namespace gm::sgm

}  // end namespace gm


namespace dds {
	template <>
	inline size_t byte_size< gm::sgm::node<qtype::SELFJOIN>* >(gm::sgm::node<qtype::SELFJOIN>  * const & ) { return 4; }
}


#endif
