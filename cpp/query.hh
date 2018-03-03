#ifndef __QUERY_HH__
#define __QUERY_HH__

#include <vector>

#include "method.hh"

/*-----------------------------------

	Descriptors for global queries

  -----------------------------------*/

namespace dds {

enum class qtype 
{
	VOID,
	SELFJOIN,
	JOIN
};


class basic_stream_query
{
public:
	typedef std::vector<stream_id> operand_tuple;

	basic_stream_query() : __type(qtype::VOID), __approx(0.0) {}
	basic_stream_query(qtype t) : __type(t), __approx(0.0) {}
	basic_stream_query(qtype t, double a) : __type(t), __approx(a) {
		assert(a>=0);
	}

	virtual ~basic_stream_query() { }

	inline qtype type() const { return __type; }
	inline void set_type(qtype t) { __type = t; }

	inline double approximation() const { return __approx; }
	inline void set_approximation(double a) { 
		assert(a>=0);
		__approx = a; 
	}

	inline const operand_tuple& operands() const { return op_sids; }
	inline void set_operands(const operand_tuple& ops) { op_sids = ops; }
	inline void set_operands(operand_tuple&& ops) { op_sids = ops; }

	inline bool exact() const { return __approx==0.0; }
	inline size_t arity() const { return op_sids.size(); }
	inline stream_id operand(size_t i) const { return op_sids.at(i); }

	bool operator==(const basic_stream_query& other) const;
	inline bool operator!=(const basic_stream_query& other) const { return !(*this == other); }
	virtual ostream& repr(ostream&) const;

protected:
	qtype __type;
	double __approx;
	operand_tuple op_sids;

};



// Short constructors for queries
basic_stream_query join(stream_id s1, stream_id s2, double beta=0.0);
basic_stream_query self_join(stream_id s, double beta=0.0);


extern binc::enum_repr<qtype> qtype_repr;


std::ostream& operator<<(std::ostream& s, const basic_stream_query& q);
std::ostream& operator<<(std::ostream& s, qtype qt);

inline std::string repr(const basic_stream_query& q)
{
	std::ostringstream S;
	S << q;
	return S.str();
}


/**
	A protocol is a simulation of a query answering method.

	This is the base class.
  */
class query_protocol : public component
{
public:
	virtual const basic_stream_query& query() const = 0;
	virtual double current_estimate() const = 0;
};



	
}  // end namespace dds


#endif