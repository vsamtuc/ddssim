
#include "query.hh"

using namespace dds;

basic_stream_query dds::join(stream_id s1, stream_id s2, double beta)
{
	if(s1==s2) return self_join(s1, beta);
	basic_stream_query Q(qtype::JOIN, beta);
	Q.set_operands({s1, s2});
	return Q;
}


basic_stream_query dds::self_join(stream_id s, double beta)
{
	basic_stream_query Q(qtype::SELFJOIN, beta);
	Q.set_operands({s});
	return Q;	
}



bool dds::basic_stream_query::operator==(const basic_stream_query& other) const
{
	return type()==other.type() && approximation()==other.approximation() 
		&& operands() == other.operands();
}


ostream& basic_stream_query::repr(ostream& s) const
{
	s << qtype_repr[type()];
	s << "(";
	bool isfirst=true;
	for(auto&& op : operands()) {
		if(!isfirst) {
			isfirst = false;
			s << ",";
		}
		s << op;
	}
	if(! exact()) {
		s << ";eps=" << approximation();
	}
	s << ")";
	return s;	
}


binc::enum_repr<qtype> dds::qtype_repr {
	{qtype::VOID, "VOID"},
	{qtype::SELFJOIN, "SELFJOIN"},
	{qtype::JOIN, "JOIN"}
};


ostream& dds::operator<<(ostream& s, qtype qt)
{
	return (s << qtype_repr[qt]);
}

ostream& dds::operator<<(ostream& s, const basic_stream_query& q)
{
	return q.repr(s);
}
