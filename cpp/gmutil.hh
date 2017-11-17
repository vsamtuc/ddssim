#ifndef __GMUTIL_HH__
#define __GMUTIL_HH__

/**
	\file Utilities common to Geometric method protocols

 */

#include "dds.hh"
#include "mathlib.hh"
#include "gm.hh"
#include "safezone.hh"

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
	This class template wraps safezone functions. It serves as part of the protocol.

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

	Vec* Eglobal;		// implementation detail, also used to compute the byte size
	size_t updates;			// number of updates, determines the size of the global sketch
	double zeta_E;			// This is acquired by calling the safezone 

	// invalid
	safezone() : szone(nullptr), Eglobal(nullptr), updates(0), zeta_E(-1) {};

	// valid safezone
	safezone(selfjoin_agms_safezone* sz, Vec* E, size_t upd, double zE)
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

	double prepare_inc(const Vec& U, double& zeta_l, double& zeta_u)
	{
		if(full()) {
			Vec X = (*Eglobal)+U;
			return szone->with_inc(incstate, X, zeta_l, zeta_u);
		} else if(naive()) {
			return zeta_l = zeta_u = zeta_E - norm_L2_with_inc(incstate_naive, U);
		} else {
			assert(!valid());
			return NAN;
		}
	}

	double operator()(delta_vector& delta, double& zeta_l, double& zeta_u)
	{
		if(full()) {
			delta += *Eglobal;
			return szone->inc(incstate, delta, zeta_l, zeta_u);
		}
		else if(naive()) {
			return zeta_l = zeta_u = zeta_E - norm_L2_inc(incstate_naive, delta);
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
			return compressed_state{*Eglobal, updates}.byte_size();
	}
};



template <qtype QType>
struct continuous_query;


template <>
struct continuous_query<qtype::SELFJOIN>
{
	typedef selfjoin_query_state query_state_type;
	typedef Vec state_vec_type;
	typedef safezone safezone_type;

	stream_id sid;
	projection proj;
	double beta;
	size_t k;

	inline size_t state_vector_size() const {
		return proj.size();
	}

	continuous_query(stream_id _sid, const projection& _proj, double _beta)
		: sid(_sid), proj(_proj), beta(_beta) 
	{
		k = CTX.metadata().source_ids().size();
	}


	delta_vector delta_update(state_vec_type& S, const dds_record& rec) 
	{
		if(rec.sid == sid) {
			delta_vector delta(proj.depth());
			auto sk = make_sketch_view(proj, S);
			sk.update(delta, rec.key, k*rec.upd);
			return delta;
		}
		return delta_vector();
	}


	bool update(state_vec_type& S, const dds_record& rec) {
		if(rec.sid == sid) {
			auto sk = make_sketch_view(proj, S);
			sk.update(rec.key, k*rec.upd);
			return true;
		}
		return false;
	}

	basic_stream_query query() const { return self_join(sid,beta); }
};



template <>
struct continuous_query<qtype::JOIN> : continuous_query<qtype::SELFJOIN>
{
	using continuous_query<qtype::SELFJOIN>::continuous_query;
};





template <template <qtype QType> class GMProto >
component* p_component_type<GMProto>::create(const Json::Value& js) 
{
	using agms::projection;

	string _name = js["name"].asString();
    stream_id _sid = js["stream"].asInt();
    projection _proj = get_projection(js);
    double _beta = js["beta"].asDouble();

    qtype qt = qtype::SELFJOIN;
    if(js.isMember("query"))
    	qt = qtype_repr[js["query"].asString()];

    switch(qt) {
    	case qtype::SELFJOIN:
    		return new GMProto<qtype::SELFJOIN>(_name, 
    			continuous_query<qtype::SELFJOIN>(_sid, _proj, _beta)
    			);
    	case qtype::JOIN:
    		return new GMProto<qtype::JOIN>(_name, 
    			continuous_query<qtype::JOIN>(_sid, _proj, _beta)
    			);
		default:
			throw std::runtime_error("Query type `"+qtype_repr[qt]+"' not supported");
    }
}




	
} // end namespace gm


#endif