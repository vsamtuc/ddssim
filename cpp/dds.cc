
#include <sstream>
#include <iterator>
#include "dds.hh"

using namespace dds;

const dds_record dds_record::zero { 0, 0, INSERT, 0, 0 };

ostream& dds::operator<<(ostream& s, dds_record const & rec)
{
	rec.repr(s);
	return s;
}


std::string named::anon(void* ptr)
{
	using namespace std;
	ostringstream S;
	S << ptr;
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

