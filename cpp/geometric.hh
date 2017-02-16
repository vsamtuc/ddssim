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
