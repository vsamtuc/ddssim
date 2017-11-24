#ifndef __GMUTIL_HH__
#define __GMUTIL_HH__

/**
	\file Protocol-related classes common to all geometric method 
	protocols.
 */

#include "dds.hh"
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

	struct __raw_record {
		dds::key_type key;
	};

	size_t byte_size() const {
		// State vectors are transmitted as floats (4 bytes)
		size_t E_size = vec.size()*sizeof(float); 

		// Raw updates are transmitted as __raw_record arrays (4 bytes)
		size_t Raw_size = sizeof(__raw_record)*updates;

		// Return the minimum of the two
		return std::min(E_size, Raw_size);
	}
};




/**
	This class transmits and accesses safezone functions.
  */
class safezone
{
	safezone_func_wrapper* szone;		// the safezone function, if any
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
	safezone(safezone_func_wrapper* sz);
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
	\brief Helper to set up a GM network for answering a query.

	This is an abstract base class, used by GM family networks to set up
	for a particular query. Subclasses implement specific behaviour.
  */
struct continuous_query
{
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



continuous_query* create_continuous_query(const Json::Value& js);


struct protocol_config
{
	bool use_cost_model = true;
};

protocol_config get_protocol_config(const Json::Value& js);


template <class GMProto>
component* p_component_type<GMProto>::create(const Json::Value& js) 
{
	string name = js["name"].asString();
	continuous_query* cq = create_continuous_query(js);
	protocol_config cfg = get_protocol_config(js);

	return new GMProto(name, cq, cfg);
}


} // end namespace gm


#endif