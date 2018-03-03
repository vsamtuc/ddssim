
#include <iterator>

#include <boost/core/demangle.hpp>

#include "binc.hh"



binc::named::named() 
	: n() 
{ }

binc::named::named(const std::string& _n) 
	: n(_n) 
{ }




std::string binc::named::anon(named const * ptr)
{
	using namespace std;
	ostringstream S;
	S << "<" << boost::core::demangle(typeid(*ptr).name()) << "@"<< ptr << ">";
	return S.str();
}



binc::basic_enum_repr::basic_enum_repr(const std::type_info& ti)
{
	set_name(boost::core::demangle(ti.name()));
}

void binc::basic_enum_repr::add(int val, const std::string& tag) 
{
	extl[val] = tag;
	intl[tag] = val;
}

int binc::basic_enum_repr::map(const std::string& tag) const 
{ 
	try {
		return intl.at(tag); 		
	} catch(std::out_of_range& ex) {
		throw std::out_of_range("enum "+name()+" does not have a tag `"+tag+"'");
	}
}

std::string binc::basic_enum_repr::map(int val) const 
{ 
	try {
		return extl.at(val); 
	} catch(std::out_of_range& ex) {
		throw std::out_of_range(binc::sprint("enum ",name(),"does not have a value equal to",val));		
	}
}

bool binc::basic_enum_repr::is_member(int val) const 
{ 
	return extl.count(val); 
}

bool binc::basic_enum_repr::is_member(const std::string& tag) const 
{ 
	return intl.count(tag);
}

