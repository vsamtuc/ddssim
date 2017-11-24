#ifndef __GM_QUERY_HH__
#define __GM_QUERY_HH__

#include "gm_proto.hh"
#include "safezone.hh"

namespace gm {


//////////////////////////////////////
//
// AGMS queries (query state and c.q.)
//
//////////////////////////////////////



// Ball safezone wrapper implementation
struct ball_safezone : safezone_func
{
	query_state* query;
	ball_safezone(query_state* q) : query(q) { }

	inline double zeta_E() const { return query->zeta_E; }

	virtual void* alloc_incstate() override;
    virtual void free_incstate(void*) override;
    virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U) override;
    virtual double compute_zeta(void* inc, const Vec& U) override;
    virtual double compute_zeta(const Vec& U) override;
    virtual size_t zeta_size() const override;
};


struct agms_query_state : query_state
{
	double beta; 		// the overall precision
	projection proj;	// the sketch projection
	double epsilon; 	// the **assumed** precision of the sketch

	agms_query_state(double _beta, projection _proj, size_t arity);
};


template <qtype QType, typename SafezoneFunc>
struct agms_join_query_state : agms_query_state
{
	static constexpr size_t arity = (QType==qtype::JOIN)? 2 : 1;

	bool eikonal;	

	agms_join_query_state(double _beta, projection _proj, bool _eikonal)
	: agms_query_state(_beta, _proj, arity), eikonal(_eikonal)
	{ 
		compute();
	}

	SafezoneFunc safe_zone;

	virtual double query_func(const Vec& x) override
	{
		switch(QType) {
			case qtype::SELFJOIN:
				return dot_est(proj(E));	
			case qtype::JOIN: 
			{
				auto x0 = std::begin(x);
				auto x1 = x0 + x.size()/2;
				auto x2 = x1 + x.size()/2;
				return dot_est(proj(x0,x1), proj(x1,x2));
			}
		}
	}

	virtual void update_estimate(const Vec& dE) override
	{
	    E += dE;
	    compute();
	}

	virtual double zeta(const Vec& X) override
	{
		return safe_zone(X);
	}

	virtual safezone_func* safezone() override
	{
		return new std_safezone_func<SafezoneFunc>(safe_zone, E.size(), E);
	}

	virtual safezone_func* radial_safezone() override
	{
		// IF EIKONAL
		return eikonal ? new ball_safezone(this) : nullptr;
	}

private:
	void compute() 
	{
		Qest = query_func(E);

		if(Qest>0) {
			Tlow =  Qest - (beta-epsilon)*fabs(Qest)/(1.0+beta);
			Thigh = Qest + (beta-epsilon)*fabs(Qest)/(1.0-beta);
		}
		else {
			Tlow = -1.0; Thigh=1.0;
		}
		safe_zone = std::move(SafezoneFunc(E, proj, Tlow, Thigh, eikonal));

		zeta_E = safe_zone(E);
	}
};

typedef agms_join_query_state<qtype::SELFJOIN, selfjoin_agms_safezone> selfjoin_query_state;
typedef agms_join_query_state<qtype::SELFJOIN, twoway_join_agms_safezone> twoway_join_query_state;



template <typename QueryState>
struct agms_continuous_query : continuous_query
{
	typedef QueryState query_state_type;
	static constexpr size_t arity = query_state_type::arity;

	std::array<stream_id, arity> sids;	// Operand streams
	projection proj;					// projection
	double beta;						// beta
	long int k;							// number of sites (for scaling)
	qtype query_type;					// for query information!

	inline size_t state_vector_size() const {
		return arity*proj.size();
	}

	agms_continuous_query(const std::vector<stream_id>& _sid, 
						const projection& _proj, double _beta,
						qtype _qtype, const query_config& _cfg)
		: proj(_proj), beta(_beta), query_type(_qtype)
	{
		config = _cfg;
		if(_sid.size()!=arity)
			throw std::length_error(binc::sprint("Expected ",arity,"operands, got",_sid.size()));
		std::copy(_sid.begin(), _sid.end(), sids.begin());
		k = CTX.metadata().source_ids().size();
	}


	inline size_t stream_operand(stream_id _sid) const {
		return std::find(sids.begin(), sids.end(), _sid)-sids.begin();
	}

	vector<stream_id> get_streams() const override {
		return std::vector<stream_id>(sids.begin(), sids.end());
	}

	delta_vector delta_update(Vec& S, const dds_record& rec) override
	{
		assert(S.size() == arity*proj.size());
		size_t opno = stream_operand(rec.sid);
		if(opno != arity) 
		{
			delta_vector delta(proj.depth());
			auto S_b = begin(S) + opno*proj.size();
			auto S_e = S_b + proj.size();
			auto sk = proj(S_b, S_e);

			// Update with scaled quantity, so that the global state is
			// independent of the number of sites
			sk.update(delta, rec.key, k*rec.upd);

			// Remember to re-base delta indices!
			delta.index += opno*proj.size();
			return delta;
		}
		return delta_vector();
	}


	bool update(Vec& S, const dds_record& rec) override
	{
		assert(S.size() == arity*proj.size());
		size_t opno = stream_operand(rec.sid);
		if(opno != arity) 
		{
			auto S_b = begin(S) + opno*proj.size();
			auto S_e = S_b + proj.size();
			auto sk = proj(S_b, S_e);
			sk.update(rec.key, k*rec.upd);
			return true;
		}
		return false;
	}

	basic_stream_query query() const override { 
		basic_stream_query q(query_type, beta);
		q.set_operands(get_streams());
		return q;
	}

	query_state* create_query_state() override {
		// So far, all query state types are constructed thus!
		// N.B. will need to change in order to support eikonality selection!

		return new query_state_type(beta, proj, config.eikonal);
	}

	double theta() const override {
		double epsilon = proj.epsilon();
		return (beta-epsilon)/(1.0-beta*beta);
	}

};



} // end namespace gm
 
#endif
