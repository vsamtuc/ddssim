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


struct selfjoin_query_state : query_state
{
	double beta; 	// the overall precision
	projection proj;// the sketch projection
	double epsilon; // the **assumed** precision of the sketch

	selfjoin_agms_safezone safe_zone;
	selfjoin_query_state(double _beta, projection _proj);

	void update_estimate(const Vec& newE) override;
	virtual double query_func(const Vec& x) override;

	virtual void* alloc_incstate() override;
	virtual void free_incstate(void*) override;
	virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U) override;
	virtual double compute_zeta(void* inc, const Vec& U) override;
	virtual double zeta(const Vec& X) override;

private:
	void compute();
};




struct twoway_join_query_state : query_state
{
	projection proj;	/// the projection
	double beta;		/// the overall precision
	double epsilon;		/// the **assumed** sketch precision

	twoway_join_agms_safezone safe_zone;	/// the safe zone object

	twoway_join_query_state(double _beta, projection _proj);

    void update_estimate(const Vec& newE) override;
	virtual double query_func(const Vec& x) override;

	virtual void* alloc_incstate() override;
	virtual void free_incstate(void*) override;
	virtual double compute_zeta(void* inc, const delta_vector& dU, const Vec& U) override;
	virtual double compute_zeta(void* inc, const Vec& U) override;
	virtual double zeta(const Vec& X) override;

protected:
	void compute();

};


template <typename QueryState, size_t QArity>
struct agms_continuous_query : continuous_query
{
	typedef QueryState query_state_type;

	std::array<stream_id, QArity> sids;	// Operand streams
	projection proj;					// projection
	double beta;						// beta
	long int k;							// number of sites (for scaling)
	qtype query_type;					// for query information!

	inline size_t state_vector_size() const {
		return QArity*proj.size();
	}

	agms_continuous_query(const std::vector<stream_id>& _sid, 
						const projection& _proj, double _beta,
						qtype _qtype)
		: proj(_proj), beta(_beta), query_type(_qtype)
	{
		if(_sid.size()!=QArity)
			throw std::length_error(binc::sprint("Expected ",QArity,"operands, got",_sid.size()));
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
		assert(S.size() == QArity*proj.size());
		size_t opno = stream_operand(rec.sid);
		if(opno != QArity) 
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
		assert(S.size() == QArity*proj.size());
		size_t opno = stream_operand(rec.sid);
		if(opno != QArity) 
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

		return new query_state_type(beta, proj);
	}

	double theta() const override {
		double epsilon = proj.epsilon();
		return (beta-epsilon)/(1.0-beta*beta);
	}

};





} // end namespace gm
 
#endif
