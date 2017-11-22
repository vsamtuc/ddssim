#ifndef __GMUTIL_HH__
#define __GMUTIL_HH__

/**
	\file Utilities common to Geometric method protocols

 */

#include "dds.hh"
#include "mathlib.hh"
#include "gm.hh"
#include "safezone.hh"
#include "binc.hh"

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
		return (szone!=nullptr) ? szone->state_size() * sizeof(float) : 0;
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
	long int k;

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
			auto sk = proj(S);
			sk.update(delta, rec.key, k*rec.upd);
			return delta;
		}
		return delta_vector();
	}


	bool update(state_vec_type& S, const dds_record& rec) {
		if(rec.sid == sid) {
			auto sk = proj(S);
			sk.update(rec.key, k*rec.upd);
			return true;
		}
		return false;
	}

	basic_stream_query query() const { return self_join(sid,beta); }
};



template <>
struct continuous_query<qtype::JOIN> 
{
	typedef twoway_join_query_state query_state_type;
	typedef Vec state_vec_type;
	typedef safezone safezone_type;

	std::array<stream_id,2> sid;
	projection proj;
	double beta;
	long int k;

	inline size_t state_vector_size() const {
		return 2*proj.size();
	}

	continuous_query(const std::array<stream_id,2>& _sid, 
						const projection& _proj, double _beta)
		: sid(_sid), proj(_proj), beta(_beta)
	{
		k = CTX.metadata().source_ids().size();
	}

	inline size_t stream_operand(stream_id _sid) const {
		return std::find(sid.begin(), sid.end(), _sid)-sid.begin();
	}

	delta_vector delta_update(state_vec_type& S, const dds_record& rec) 
	{
		size_t opno = stream_operand(rec.sid);
		assert(opno >=0 && opno<=2);
		if(opno != 2) 
		{
			delta_vector delta(proj.depth());
			auto S_b = begin(S) + opno*proj.size();
			auto S_e = S_b + proj.size();
			auto sk = proj(S_b, S_e);
			sk.update(delta, rec.key, k*rec.upd);

			// Remember to re-base delta
			if(opno == 1) delta.index += proj.size();
			return delta;
		}
		return delta_vector();
	}


	bool update(state_vec_type& S, const dds_record& rec) {
		size_t opno = stream_operand(rec.sid);
		assert(opno >=0 && opno<=2);
		if(opno != 2) 
		{
			auto S_b = begin(S) + opno*proj.size();
			auto S_e = S_b + proj.size();
			auto sk = proj(S_b, S_e);
			sk.update(rec.key, k*rec.upd);
			return true;
		}
		return false;
	}

	basic_stream_query query() const { return join(sid[0], sid[1], beta); }
};





template <template <qtype QType> class GMProto >
component* p_component_type<GMProto>::create(const Json::Value& js) 
{
	using agms::projection;

	string _name = js["name"].asString();
    vector<stream_id> sids = get_streams(js);
    projection _proj = get_projection(js);
    double _beta = js["beta"].asDouble();

    qtype qt = qtype::SELFJOIN;
    if(js.isMember("query"))
    	qt = qtype_repr[js["query"].asString()];

    switch(qt) {
    	case qtype::SELFJOIN:
    		
    		if(sids.size()!=1) 
    			throw std::invalid_argument(binc::sprint("One stream expected, got ",sids.size()));

    		return new GMProto<qtype::SELFJOIN>(_name, 
    			continuous_query<qtype::SELFJOIN>(sids[0], _proj, _beta)
    			);
    	case qtype::JOIN:
    		if(sids.size()!=2) 
    			throw std::invalid_argument(binc::sprint("Two streams expected, got ",sids.size()));

    		return new GMProto<qtype::JOIN>(_name, 
    			continuous_query<qtype::JOIN>(std::array<stream_id,2> {sids[0], sids[1]}, _proj, _beta)
    			);
		default:
			throw std::runtime_error("Query type `"+qtype_repr[qt]+"' not supported");
    }
}




	
} // end namespace gm


#endif