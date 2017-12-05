#ifndef __GM_PROTO_HH__
#define __GM_PROTO_HH__

/**
	\file Protocol-related classes common to all geometric method 
	protocols.
 */

#include "dds.hh"
#include "dsarch.hh"
#include "method.hh"
#include "results.hh"
#include "hdv.hh"
#include "gm.hh"
#include "gm_szone.hh"

namespace gm {

using namespace dds;


/**
	Wrapper for a state vector and number of updates.

	This class wraps a reference to a state vector together with 
	a count of the updates it contains. The byte size of this 
	object is computed to be the minimum of the size of the
	sketch and the size of all the updates.
  */
struct compressed_state
{
	const Vec& vec;
	size_t updates;


	size_t byte_size() const {
		// State vectors are transmitted as floats (4 bytes)
		size_t E_size = vec.size()*sizeof(float); 

		// Raw updates are transmitted as stream_update arrays (8 bytes)
		size_t Raw_size = sizeof(dds::stream_update)*updates;

		// Return the minimum of the two
		return std::min(E_size, Raw_size);
	}
};




/**
	This class wraps safezone_funct objects for transmission and access.

	It is essentially a wrapper for the more verbose, polymorphic \c safezone_func API,
	but it conforms to the standard functional API. It is copyable and moveable
	In addition, it provides a byte_size() method, making it suitable for integration
	with the middleware.
  */
class safezone
{
	safezone_func* szone;		// the safezone function, if any
	void*  inc; 						// pointer to inc state if any

	inline void* get_inc() {
		if(inc==nullptr && szone!=nullptr)
			inc = szone->alloc_incstate();
		return inc;
	}

	inline void clear_inc() {
		if(inc != nullptr) {
			assert(szone);
			szone->free_incstate(inc);
			inc = nullptr;
		}		
	}

public:
	/// null state
	safezone();

	/// valid safezone
	safezone(safezone_func* sz);
	~safezone();

	/// Movable
	safezone(safezone&&);
	safezone& operator=(safezone&&);

	/// Copyable
	safezone(const safezone& );
	safezone& operator=(const safezone&);

	inline void swap(safezone& other) {
		std::swap(szone, other.szone);
		std::swap(inc, other.inc);
	}

	inline double operator()(const Vec& U)
	{
		return (szone!=nullptr) ? szone->compute_zeta(get_inc(), U) : NAN;
	}

	inline double operator()(const delta_vector& delta, const Vec& U)
	{
		return (szone!=nullptr) ? szone->compute_zeta(get_inc(), delta, U) : NAN;
	}

	inline size_t byte_size() const {
		return (szone!=nullptr) ? szone->zeta_size() * sizeof(float) : 0;
	}
};



/**
	Labels for rebalancing algorithms.
  */
enum class rebalancing
{
	none,
	random,
	random_limits,
	projection,
	random_projection
};

extern enum_repr<rebalancing> rebalancing_repr;


/**
	Query and protocol configuration.
  */
struct protocol_config
{
	bool use_cost_model = true;		// for fgm: use the cost model if possible
	bool eikonal = true;			// select eikonal safe zone
	rebalancing rebalance_algorithm
			 = rebalancing::none;	// select rebalancing algorithm
	size_t rbl_proj_dim;			// the rebalancing projection dimension
};



/**
	\brief Helper to set up a GM network for answering a query.

	This is an abstract base class, used by GM family networks to set up
	for a particular query. Subclasses implement specific behaviour.
  */
struct continuous_query
{
	// These are attributes requested by the user
	protocol_config config;


	virtual ~continuous_query() { }

	/**
		\brief Return an initialized \c query_state object.

		This is a factory method, returning a new object each time it is called.
		The caller is responsible for destroying the object after it is used.
	  */
	virtual query_state* create_query_state()=0;

	/**
		\brief Return the size of the state vector
	  */
	virtual size_t state_vector_size() const =0;

	/**
		\brief Safe zone monitoring accuracy.

		This parameter describes the width of safezones relative to the
		monitored function.

		Note that this is not the same as query accuracy. For example, assume
		that we have a sketch monitoring a query at accuracy \f$\epsilon\f$.
		If we monitor the sketch function with a safe zone of size \f$\theta\f$,
		then the total query accuracy \f$\beta \f$ is roughly 
		\f$ \beta = \epsilon + \theta \f$.

		In general, this parameter represents the 'breadth' of safezones, in a 
		highly problem specific manner. It is mostly useful for reporting.
	  */
	virtual double theta() const =0;

	/**
		\brief Return the set of stream ids for this query.
	  */
	virtual std::vector<stream_id> get_streams() const =0;

	/**
		\brief Return a stream_query object describing the query
		function.
	  */
	virtual basic_stream_query query() const =0;

	/**
		\brief Apply an update to a state vector and return a delta.
		
	  */
	virtual delta_vector delta_update(Vec& S, const dds_record& rec)=0;

	/**
		\brief Appl
	  */
	virtual bool update(Vec& S, const dds_record& rec)=0;

};



/**
	A channel implementation which accounts a combined network cost.

	The network cost is computed as follows: 
	for each transmit of b bytes, there is a total charge of
	header * ceiling(b/MSS) bytes.

	This cost is resembling the TPC segment cost.
  */
struct tcp_channel : channel
{
	static constexpr size_t tcp_header_bytes = 40;
	static constexpr size_t tcp_mss = 1024;

	tcp_channel(host* src, host* dst, rpcc_t endp);

	void transmit(size_t msg_size) override;

	inline size_t tcp_bytes() const { return tcp_byts; }

protected:
	size_t tcp_byts;

};



/**
	A result table for geometric protocols
  */

struct gm_comm_results_t : result_table, dataset_results, comm_results
{

	column_ref<string> run_id	  {this, "run_id", 64, "%s", CTX.run_id };

	column<string> name   	  	  {this, "name", 64, "%s" };
	column<string> protocol   	  {this, "protocol", 64, "%s" };
	column<string> query   	  	  {this, "query", 80, "%s" };

	column<double> max_error 	  {this, "max_error", "%.8g" };
	column<size_t> statevec_size  {this, "statevec_size", "%zu" };
	
	column<size_t> sites     	  {this, "sites", "%zu" };
	
	column<size_t> rounds 			{this, "rounds", "%zu" };
	column<size_t> subrounds		{this, "subrounds", "%zu" };
	column<size_t> sz_sent			{this, "sz_sent", "%zu"};
	column<size_t> total_updates	{this, "total_updates", "%zu"};	
	column<size_t> total_rbl_size	{this, "total_rbl_size", "%zu"};

	column<size_t> bytes_get_drift	{this, "bytes_get_drift", "%zu"};
	column<size_t> tcp_traffic		{this, "tcp_traffic", "%zu"};
	column<double> tcp_traffic_pct	{this, "tcp_traffic_pct", "%.10g"};

	gm_comm_results_t() 
		: gm_comm_results_t("gm_comm_results")
	{}
	gm_comm_results_t(const string& name) ;
	
	template <typename StarNetwork>
	void fill(StarNetwork* nw)
	{
		comm_results::fill(nw);
		name = nw->name();
		protocol = nw->rpc().name();
		query = repr(nw->Q->query());

		max_error = nw->Q->theta();
		statevec_size = nw->Q->state_vector_size();
		sites = nw->sites.size();

		auto hub = nw->hub;
		rounds = hub->num_rounds;
		subrounds = hub->num_subrounds;
		sz_sent = hub->sz_sent;
		total_updates = hub->total_updates;
		total_rbl_size = hub->total_rbl_size;

		// number of bytes received by get_drift()
		bytes_get_drift = chan_frame(nw)
			.endp(typeid(typename StarNetwork::site_type),"get_drift")
			.endp_rsp()
			.bytes();

		size_t tcp_traf = chan_frame(nw)
			.tally<size_t>([](channel* c) { return static_cast<tcp_channel*>(c)->tcp_bytes(); });

		tcp_traffic = tcp_traf;

		double tcp_naive_traffic =  (tcp_channel::tcp_header_bytes + sizeof(dds::stream_update))*CTX.stream_count();
		tcp_traffic_pct = (double)tcp_traf / tcp_naive_traffic;
	}
};
extern gm_comm_results_t gm_comm_results;




/**
	A base class for GM family networks.

	Subclasses inherit from this class in order to further customize the
	behaviour.
  */
template <typename Net, typename Coord, typename Node>
struct gm_network : star_network<Net, Coord, Node>, component
{
	typedef Coord coordinator_t;
	typedef Node node_t;
	typedef Net network_t;
	typedef star_network<network_t, coordinator_t, node_t> star_network_t;

	continuous_query* Q;
	
	const protocol_config& cfg() const { return Q->config; }

	gm_network(const string& _name, continuous_query* _Q)
	: star_network_t(CTX.metadata().source_ids()), Q(_Q)
	{ 
		this->set_name(_name);
		this->setup(Q);

		on(START_STREAM, [&]() { 
			process_init(); 
		} );
		on(END_STREAM, [&]() { 
			process_fini(); 
		} );
		on(START_RECORD, [&]() { 
			process_record(); 
		} );
		on(RESULTS, [&](){ 
			output_results();
		});
		on(INIT, [&]() {
			CTX.timeseries.add(this->hub->Qest_series);
		});
		on(DONE, [&]() { 
			CTX.timeseries.remove(this->hub->Qest_series);
		});
	}


	channel* create_channel(host* src, host* dst, rpcc_t endp) const override
	{
		if(! dst->is_mcast())
			return new tcp_channel(src, dst, endp);
		else
			return basic_network::create_channel(src, dst, endp);
	}

	void process_record()
	{
		const dds_record& rec = CTX.stream_record();
		this->source_site(rec.hid)->update_stream();		
	}

	virtual void process_init()
	{
		// let the coordinator initialize the nodes
		this->hub->warmup();
		this->hub->start_round();
	}

	virtual void process_fini()
	{
		this->hub->finish_rounds();
	}

	virtual void output_results()
	{
		network_comm_results.fill_columns(this);
		network_comm_results.emit_row();

		network_host_traffic.output_results(this);
		network_interfaces.output_results(this);

		gm_comm_results.fill(this);
		gm_comm_results.emit_row();
	}

	~gm_network() 
	{ delete Q; }
};



/**
	Returns a \c continuous_query object specified by the given component json.
 */
continuous_query* create_continuous_query(const Json::Value& js);


/**
	Returns a \c protocol_config object specified by the given component json.

	This is called internally by \c create_continuous_query
 */
protocol_config get_protocol_config(const Json::Value& js);


/**
	Factory method for GM components.
  */
template <class GMProto>
component* p_component_type<GMProto>::create(const Json::Value& js) 
{
	string name = js["name"].asString();
	continuous_query* cq = create_continuous_query(js);

	return new GMProto(name, cq);
}


} // end namespace gm


#endif