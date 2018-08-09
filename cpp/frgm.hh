#ifndef __FGM_HH__
#define __FGM_HH__

#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <cmath>
#include <vector>

#include "gm_proto.hh"

namespace gm { namespace frgm {

using std::cout;
using std::endl;
using std::vector;
using namespace dds;


struct coordinator;
struct node;
struct node_proxy;

struct network 
	: 	gm_network<network , coordinator , node>
{
	typedef gm_network<network_t, coordinator_t, node_t> gm_network_t;

	network(const string& name, continuous_query* _Q);
};


//////////////////////////////////
// 
// Query, state, safezone
//
//////////////////////////////////


/**
	This is an implementation of a cost model for deciding whether to ship 
	a full safe zone \f$\zeta\f$ or just a cheap safe \f$\zeta_B\f$ zone to the local sites.

	The inputs to the model are three arrays (i=0...k-1)
	- alpha[i] is the rate at which \f$\zeta(U_i)\f$ decreases as a function of "time"
	- beta[i] is the rate at which \f$\zeta_B(U_i)\f$ decreases as a function of "time"
	- gamma[i] is the percent of stream updates 

	The output is a boolean array d[i] which is true iff the full safe zone should be
	sent to site i.
  */
struct cost_model 
{
	coordinator* coord;
	size_t k;

	// model input
	size_t D;				// The cost of shipping full safezones
	vector<bool> proper;    // designates which sites to process
	double total_alpha, total_beta, round_updates; // totals
	Vec alpha, beta, gamma; // source models

	// model output
	vector<bool> d;		// the model's output plan
	double max_gain;	// predicted gain for plan
	double tau_opt;		// predicted tau for plan

	// Initialize the vectors holding the model parameters
	cost_model(coordinator* _coord);

	/**
		Called at the end of a round to update the model input arrays.
	  */
	void update_model();	// update the model statistics

	/**
		Given values in the model arrays, computes the model output d[]
	  */
	void compute_model();	// compute the model statistics

	/**
		Simply prints the arrays
	  */
	void print_model();		// print the model
};



struct coordinator : process
{
	typedef coordinator coordinator_t;
	typedef node node_t;
	typedef node_proxy node_proxy_t;
	typedef network network_t;

	proxy_map<node_proxy_t, node_t> proxy;

	//
	// protocol stuff
	//

	continuous_query* Q; 		// continuous query
	query_state* query;			// current query state
	safezone_func *safe_zone;	// the safe zone proper
	safezone_func *radial_safe_zone;	// the cheap safezone (maybe null)
	
	bool using_cost_model;		// flag to designate use of the cost model

	size_t k;					// number of sites

	// index the nodes
	map<node_t*, size_t> node_index;
	vector<node_t*> node_ptr;

	//--------------------------
	// protocol related
	//--------------------------

	// per-node bit weight and total bitweight
	vector<int> bitweight, total_bitweight;

	int bit_budget;		// the current number of available bits
	int bit_level;		// the current bit-level (# of subround?)

	set<node*> B;		// Balance set
	Vec DeltaEbal;		// The sum of Ei-E0 for i in B
	double psi_Ebal;	// Balance weight == $(|B|/2) \zeta(E_0 + (2/|B|) \Delta E_{bal})$.

	/*
		At every point in time, we have  DS = X + DeltaEbal  =  lambda*(X  / lambda) + mu (DeltaEbal / k*mu).
		with \lambda , \mu >= 0 and \lambda+ \mu = 1.

		Choosing mu so that psi_Ebal = zeta( DeltaEbal / k*mu ), and defining
		 \psi = \sum \zeta(X_i / \lambda)  

		the condition becomes
		 \lambda psi + \mu \psi_Ebal  \geq 0, or, 

		\psi \geq 0.
 	 */
	double lambda, mu;
	double epsilon_psi = 0.01;

	//-----------------------------------------
	// timeseries
	//-----------------------------------------

	// reports the series of query estimate
	computed<double> Qest_series;

	// statistics
	size_t num_rounds;      // number of rounds
	size_t num_subrounds;   // number of subrounds
	size_t sz_sent;         // safe zones sent
	size_t total_rbl_size; 	// total size of rebalance sets
	size_t round_sz_sent;   // safezones sent in current round
	size_t total_updates;	// number of stream updates received

	// cost model
	cost_model cmodel;
	
	//------------------------------------
	// methods
	//------------------------------------

	coordinator(network_t* nw, continuous_query* _Q);
	~coordinator();

	inline network_t* net() const { return static_cast<network_t*>(host::net()); }
	inline const protocol_config& cfg() const { return Q->config; }

	void setup_connections() override;

	// load the warmup dataset
	void warmup();

	//
	// State transitions (except for threshold)
	//

	// remote call on host violation
	oneway threshold_crossed(sender<node_t> ctx, int delta_bitw);

	void start_round();   // initialize a new round
	void restart_round(); // finish current round and start new one
	void finish_round();  // finish current round
	void finish_with_newE(const Vec& newE); // finishes round resetting with new state
	void finish_rounds();  // finish last round

		
	void start_subround(double psi);    // initialize a new subround
	void finish_subround();                    // finish the subround
	void finish_subrounds(double psi);  // try to rebalance (optional)

	// attempting to rebalance
	void collect_drift_vectors(double& psi, size_t& upd);	// collect drift vectors before round end

	// rebalancing algorithms, return true success
	bool rebalance_bimodal(double& psi);		// rebalance mode is bimodal
	bool rebalance_zero_balance(double& psi);	// rebalance mode is zero_balance

	// calls the appropriate rebalancing algorithm, returns true on success
	bool rebalanced(double& psi);

	// helper function, sends new lambda to all sites, returns new psi
	double collect_psi(double lambda);

	//
	// this is used to trace the execution of rounds, for debugging or tuning
	//
	void trace_round(const Vec& newE);
	// this is used to print the state of nodes
	void print_state();
	
};



struct coord_proxy : remote_proxy< coordinator >
{
	typedef coordinator coordinator_t;
	REMOTE_METHOD(coordinator_t, threshold_crossed);
	coord_proxy(process* c) : remote_proxy< coordinator_t >(c) { }
};




/**
	The FRGM node
  */
struct node : local_site
{
	typedef coordinator coordinator_t;
	typedef node node_t;
	typedef node_proxy node_proxy_t;
	typedef network network_t;
	typedef coord_proxy coord_proxy_t;

	continuous_query* Q;	// the query management object
	safezone szone;			// safezone object

	int num_sites;			// number of sites

	double minzeta; 		// minimum value of zeta so far
	double zeta;			// current zeta 

	double zeta_0;			// start value for discretization, equal to zeta at last reset_bitweight()
	double zeta_quantum;	// discretization for bitweight, set by reset_bitweight()
	int bitweight;			// equal to number of bits sent since last reset_bitweight()

	double lambda;			// the current lambda scaling factor for U.

	Vec U;					// drift vector
	Vec Uinc;				// This vector maintains the value needed for incremental computation.
							// It is equal to U/lambda

	size_t update_count;	// number of updates in drift vector

	Vec dS;						// the vector of all updates over a round
	size_t round_local_updates; // number of local stream updates since last reset

	coord_proxy_t coord;

	node(network_t* net, source_id hid, continuous_query* _Q);

	void setup_connections() override;

	void update_stream();

	//-----------------------------------
	// Remote methods
	//-----------------------------------

	// Called at the start of a round
	oneway reset(const safezone& newsz);

	// Get the current zeta		
	float get_zeta();

	// called at the start a new subround
	oneway reset_bitweight(float Z);

	// called after rebalancing
	double reset_lambda(float _lambda);

	// Get the drift vector
	compressed_state_obj flush_drift();

};


struct node_proxy : remote_proxy< node >
{
	typedef node node_t;

	REMOTE_METHOD(node_t, reset);
	REMOTE_METHOD(node_t, reset_bitweight);
	REMOTE_METHOD(node_t, reset_lambda);
	REMOTE_METHOD(node_t, get_zeta);
	REMOTE_METHOD(node_t, flush_drift);

	node_proxy(process* p) : remote_proxy< node_t >(p) {}
};

} // end namespace gm::fgm

}  // end namespace gm


namespace dds{
	
template <>
inline size_t byte_size< gm::frgm::node *>
	(gm::frgm::node * const &) { return 4; }

using hdv::Vec;

template <>
inline size_t byte_size< Vec >
	(Vec const & v) { return sizeof(float)* v.size(); }

}



#endif
