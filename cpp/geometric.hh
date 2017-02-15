#ifndef __GEOMETRIC_HH__
#define __GEOMETRIC_HH__

#include <iostream>
#include <unordered_map>
#include <memory>
#include <algorithm>

#include "dsarch.hh"
#include "agms.hh"

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
	network(projection _proj) {
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

struct network;
struct coordinator;


struct selfjoin_query
{
	double beta; 	// the overall precision
	sketch E;   	// The current estimate for the stream

	double Qest; 	// current estimate
	double Tlow;    // admissible region: Tlow <= Q(t) <= Thigh
	double Thigh;   // 


	selfjoin_query(double _beta, projection _proj)
	: beta(_beta), E(_proj)
	{
		assert(_proj.epsilon() < beta);
		Qest = 0.0;

		// initialize the current estimate to 0
		Tlow = Thigh = Qest = 0.0;
	}


	void update_estimate(const sketch& newE)
	{
		// compute the admissible region
		E = newE;
		Qest = dot_est(E);

		Tlow = (1+E.proj.epsilon())*Qest/(1.0+beta);
		Thigh = (1-E.proj.epsilon())*Qest/(1.0-beta);
	}
};



/**
	Base function for boolean quantile safezones.
	This can be used to prepare a safezone function
	for quantile queries, both eikonal and non-eikonal.
  */
struct quantile_safezone
{
	size_t n;	// the number of inputs
	Index L;  	// the legal inputs map from n to zetaE
	Vec zetaE;	// the reference vector's zetas, where zE >= 0.

	quantile_safezone() : n(0) { }

	quantile_safezone(const Vec& zE) 
	: n(zE.size())
	{
		// count legal inputs
		size_t Legal[n];
		size_t pos = 0;
		for(size_t i=0; i<n; i++)
			if(zE[i]>0) Legal[pos++] = i;

		// create the index and the other matrices
		Index Ltmp(Legal, pos);
		Ltmp.swap(L);
		zetaE.resize(L.size());
		zetaE = zE[L];
	}

	quantile_safezone(quantile_safezone&&) = default;
	quantile_safezone& operator=(quantile_safezone&&) = default;
};


/**
	Computing the safe zone function for the quantile
	boolean function, Q_k,n (x_1, ... , x_n), which is
	true where at least k,  1\leq k \leq n, of the 
	x_i's are true.

	For k=1, this is equivalent to the "or" and
	for k=n this is equivalent to the "and".

	The resulting function is non-eikonal and can be
	computed in time O(n).
  */
struct quantile_safezone_non_eikonal : quantile_safezone
{
	size_t k;	// the lower bound on true inputs

	quantile_safezone_non_eikonal() { }

	quantile_safezone_non_eikonal(quantile_safezone&& qsz, size_t _k) 
	: k(_k) 
	{
		this->quantile_safezone::operator=(std::move(qsz));
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");		
	}

	quantile_safezone_non_eikonal(const Vec& zE, size_t _k) 
	: quantile_safezone(zE), k(_k)
	{
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");
	}

	double operator()(const Vec& zX) {
		Vec zEzX = zetaE;
		zEzX *= zX[L];
		std::nth_element(begin(zEzX), begin(zEzX)+(L.size()-k), end(zEzX));
		return std::accumulate(begin(zEzX), begin(zEzX)+(L.size()-k+1), 0.0);
	}

};


/**
	Computing the safe zone function for the quantile
	boolean function, Q_k,n (x_1, ... , x_n), which is
	true where at least k,  1\leq k \leq n, of the 
	x_i's are true.

	For k=1, this is equivalent to the "or" and
	for k=n this is equivalent to the "and".

	The resulting function is eikonal and can be
	computed in time O(n*2^n).
  */
struct quantile_safezone_eikonal : quantile_safezone
{
	size_t k;	// the lower bound on true inputs

	quantile_safezone_eikonal() { }

	quantile_safezone_eikonal(quantile_safezone&& qsz, size_t _k) 
	: k(_k) 
	{
		this->quantile_safezone::operator=(std::move(qsz));
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");
	}

	quantile_safezone_eikonal(const Vec& zE, size_t _k) 
	: quantile_safezone(zE), k(_k)
	{
		assert(1<=k && k<=n);
		if(L.size()<k)
			throw std::length_error("The reference vector is non-admissible");
	}


	inline static double zeta_I(size_t *I, size_t m, const Vec& zEzX, const Vec& zEzE)
	{
		double num = 0.0;
		double denom = 0.0;
		for(size_t i=0; i<m; i++) {
			num += zEzX[I[i]];
			denom += zEzE[I[i]];
		}
		//cout << "I=" << Index(I,m) << "  zEzX=" << zEzX << "   zEzE=" << zEzE << " num=" << num << " denom=" << sqrt(denom) << endl;
		return num/sqrt(denom);
	}

	/**
		Helper to iterate over all strictly increasing m-long
		sequences over [0:l)
	  */
	inline static bool next_I(size_t *I, size_t m, size_t l) 
	{
		for(size_t i=1; i<=m; i++) {
			if(I[m-i] < l-i) {
				I[m-i]++;
				for(size_t j=1;j<i;j++) 
					I[m-i+j] = I[m-i]+j;
				return true;
			}
		}
		return false;
	}


	double operator()(const Vec& zX) {
		Vec zEzX = zetaE;
		zEzX *= zX[L];
		Vec zEzE = zetaE*zetaE;

		// find the minimum over all m-sets from [0:L.size())
		size_t l = L.size();
		size_t m = L.size()-k+1;

		size_t I[m];
		std::iota(I, I+m, 0);

		double zinf = zeta_I(I, m, zEzX, zEzE);
		while(next_I(I, m, l)) {
			double zI = zeta_I(I, m, zEzX, zEzE);
			zinf = std::min(zI, zinf);
		}

		return zinf;
	}
};



/**
	The safe zone function for the selfjoin estimate of a sketch.

	The admissible region is \f$ A =\{ X | \median{X_i^2}  \leq T \}\f$.

	The safe zone is defined as follows: 
	zeta_i(U_i)  =   sqrt(T) - ||E_i+U_i||

	zeta = inf_{I in (D choose K)}  [ \sum_{i\in I} zeta_i(0)*\zeta_i(U_i) ]

	TODO: Make it incremental! Current complexity is O(|sketch|)
  */
struct selfjoin_agms_safezone_upper_bound
{
	sketch& E;		// reference vector
	double T;		// threshold above

	quantile_safezone_eikonal Median;


	selfjoin_agms_safezone_upper_bound(sketch& _E, double _T)
	: 	E(_E), T(_T), 
		Median(sqrt(T) - sqrt(dot_estvec(E)), (E.depth()+1)/2)
	{ }

	inline double operator()(const sketch& U) 
	{
		Vec z = sqrt(T) - sqrt(dot_estvec(E+U));
		return Median(z);
	}
};



/**
	The safe zone function for the selfjoin estimate of a sketch.

	The admissible region is \f$ A =\{ X | \median{X_i^2}  \geq T \}\f$.

	The safe zone is defined as follows: 
	zeta_i(U_i)  =   U \frac{E}{\norm{E}} + \norm{E} - \sqrt{T}

	zeta = inf_{I in (D choose K)}  [ \sum_{i\in I} zeta_i(0)*\zeta_i(U_i) ]
  */
struct selfjoin_agms_safezone_lower_bound
{
	sketch& E;		// reference vector
	double T;		// threshold above

	quantile_safezone_eikonal Median;  // used to compose the median
	Vec norm_E_estvec;	// cached for speed


	selfjoin_agms_safezone_lower_bound(sketch& _E, double _T)
	: 	E(_E), T(_T), 
		Median(sqrt(dot_estvec(E)) - sqrt(T), (E.depth()+1)/2),
		norm_E_estvec( sqrt(dot_estvec(E,E)) )
	{ }

	inline double operator()(const sketch& U) 
	{
		Vec z = dot_estvec(U,E)/norm_E_estvec + norm_E_estvec - sqrt(T);
		return Median(z);
	}
};




struct selfjoin_agms_safezone
{
	selfjoin_query& query;		// the query
	sketch& E;					// keep a reference to query E
	isketch& U;  				// the drift vector


	selfjoin_safezone(selfjoin_query& q, isketch& _U) 
	: query(q), E(q.E), U(_U)
	{
		precompute_zone();
	}

	// Called when E, Thigh and Tlow change,
	// to prepare some incremental estimates
	void precompute_zone()
	{
		// compute the norms of sketch columns
		// norm_E = sqrt(dot_estvec(E));
		// update_directly();
	}
};






struct coordinator : process
{
	coordinator(network*, const projection&);

};




struct node : local_site
{
	node(network*, source_id hid, const projection& _proj);
};




struct network : star_network<network, coordinator, node>
{
	projection proj;
	double beta;

	network(const projection& _proj, double _beta)
	: proj(_proj), beta(_beta) 
	{
		setup(proj);
	}

};






} }  // end namespace dds::gm




#endif
