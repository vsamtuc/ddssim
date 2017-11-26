
#include <sstream>
#include <iterator>

#include <boost/core/demangle.hpp>

#include "binc.hh"
#include "dds.hh"

using namespace dds;

const dds_record dds_record::zero { 0, 0, 0, 0, 0};

ostream& dds::operator<<(ostream& s, dds_record const & rec)
{
	rec.repr(s);
	return s;
}



named::named() 
	: n() 
{ }

named::named(const std::string& _n) 
	: n(_n) 
{ }




std::string named::anon(named const * ptr)
{
	using namespace std;
	ostringstream S;
	S << "<" << boost::core::demangle(typeid(*ptr).name()) << "@"<< ptr << ">";
	return S.str();
}



void ds_metadata::merge(const ds_metadata& other)
{
	using std::min;
	using std::max;
	isvalid = isvalid || other.isvalid;
	scount += other.scount;

	kmin = min(kmin, other.kmin);
	kmax = max(kmax, other.kmax);

	ts = min(ts, other.ts);
	te = max(te, other.te);

	sids.insert(other.sids.begin(), other.sids.end());
	hids.insert(other.hids.begin(), other.hids.end());
}


dds::basic_enum_repr::basic_enum_repr(const std::type_info& ti)
{
	set_name(boost::core::demangle(ti.name()));
}

void dds::basic_enum_repr::add(int val, const std::string& tag) 
{
	extl[val] = tag;
	intl[tag] = val;
}

int dds::basic_enum_repr::map(const std::string& tag) const 
{ 
	try {
		return intl.at(tag); 		
	} catch(std::out_of_range& ex) {
		throw std::out_of_range("enum "+name()+" does not have a tag `"+tag+"'");
	}
}

std::string dds::basic_enum_repr::map(int val) const 
{ 
	try {
		return extl.at(val); 
	} catch(std::out_of_range& ex) {
		throw std::out_of_range(binc::sprint("enum ",name(),"does not have a value equal to",val));		
	}
}

bool dds::basic_enum_repr::is_member(int val) const 
{ 
	return extl.count(val); 
}

bool dds::basic_enum_repr::is_member(const std::string& tag) const 
{ 
	return intl.count(tag);
}



