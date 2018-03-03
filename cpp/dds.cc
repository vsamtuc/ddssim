

#include "binc.hh"
#include "dds.hh"

using namespace dds;

const dds_record dds_record::zero { 0, 0, 0, 0, 0};

ostream& dds::operator<<(ostream& s, dds_record const & rec)
{
	rec.repr(s);
	return s;
}




