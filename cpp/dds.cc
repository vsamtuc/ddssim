
#include <sstream>
#include <iterator>

#include <boost/core/demangle.hpp>

#include "dds.hh"

using namespace dds;

const dds_record dds_record::zero { 0, 0, 0, 0, 0};

ostream& dds::operator<<(ostream& s, dds_record const & rec)
{
	rec.repr(s);
	return s;
}


std::string named::anon(named const * ptr)
{
	using namespace std;
	ostringstream S;
	S << "<" << boost::core::demangle(typeid(*ptr).name()) << " @ "<< ptr << ">";
	return S.str();
}

bool dds::operator==(const basic_query& q1, const basic_query& q2)
{
	if(q1.type == q2.type) {
		switch(q1.type) {
			case qtype::VOID:
				return true;
			case qtype::SELFJOIN:
				return 
					query_cast<qtype::SELFJOIN>(q1).param
					==
					query_cast<qtype::SELFJOIN>(q2).param;
			case qtype::JOIN:
				return 
				query_cast<qtype::JOIN>(q1).param
				==
				query_cast<qtype::JOIN>(q2).param;
			default:
				throw std::runtime_error("unhandled");
		}
	}
	return false;
}


ostream& dds::operator<<(ostream& s, qtype qt)
{
	switch(qt) {
		case qtype::VOID: {
			return (s << "void_query");
		}
		case qtype::SELFJOIN:
			return (s << "selfjoin");
		case qtype::JOIN:
			return (s << "join");
		default:
			throw std::runtime_error("unhandled");
	}	
}

ostream& dds::operator<<(ostream& s, const basic_query& q)
{
	s << q.type;
	switch(q.type) {
		case qtype::VOID: {
			return s;
		}
		case qtype::SELFJOIN:
			return (s << "_" << 
				query_cast<qtype::SELFJOIN>(q).param);
		case qtype::JOIN:
			return (s << "_" <<
				query_cast<qtype::JOIN>(q).param.first << "_" <<
				query_cast<qtype::JOIN>(q).param.second);
		default:
			throw std::runtime_error("unhandled");
	}	
}
