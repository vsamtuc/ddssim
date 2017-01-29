
#include <sstream>
#include <iterator>
#include "dds.hh"

const dds::dds_record dds::dds_record::zero { 0, 0, INSERT, 0, 0 };

std::string dds::named::anon(void* ptr)
{
	using namespace std;
	ostringstream S;
	S << ptr;
	return S.str();
}


