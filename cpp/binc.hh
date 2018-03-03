#ifndef __BINC_HH__
#define __BINC_HH__


#include <map>
#include <iostream>
#include <sstream>
#include <boost/lexical_cast.hpp>

/*
	In the global namespace
*/


struct __print_control
{
	std::string _sep=" ";
	bool start = true;
	std::ostream* s = &std::cout;

	template <typename T>
	int handle(const T& arg) {
		if(start) { start = false; } else { *s << _sep; }
		*s << arg;
		return 0;
	}
};

// Used to change the separator
struct sep
{
	std::string _sep;
	sep(const std::string& s=" ") : _sep(s) {}
};


template <>
inline int __print_control::handle<sep>(const sep& thesep) {
	_sep = thesep._sep; return 0;
}




/*
	This is code in the binc namespace (binc == "batteries included")
*/


namespace binc {


/*******************************************
 *
 *  Convenient print and sprint
 *
 *******************************************/

using namespace std;



template <typename Iter1, typename Iter2>
struct __element_range
{
	Iter1 iter1;
	Iter2 iter2;
	std::string sep;
	__element_range(Iter1 i1, Iter2 i2, const std::string& _sep) : iter1(i1), iter2(i2), sep(_sep) {}
};


/**
	Returns a wrapper for printing the elements of an iterable object.
  */
template <typename Iterable>
inline auto elements_of(const Iterable& container, const std::string& sep=" ") {
	return __element_range<decltype(std::begin(container)),decltype(std::end(container)) >
		(std::begin(container), std::end(container), sep);
}

template <typename Iter1, typename Iter2>
inline std::ostream& operator<< (std::ostream& s, __element_range<Iter1, Iter2> elem_range)
{
	bool start = true;
	for(auto I=elem_range.iter1; I!=elem_range.iter2; ++I) {
		if(start) { start=false; } else { s << elem_range.sep; }
		s << *I;
	}
	return s;
}


/**
	The main workhorse. Just print...
  */
template <typename ...Args>
void print(const Args&...args)
{
	__print_control cfg;
	typedef int swallow[];
    (void) swallow {0,
            ( cfg.handle(args) )...
    };
    std::cout << std::endl;
}

/**
  Just print to a string (used for exception messages etc)... 
  */
template <typename ...Args>
std::string sprint(Args...args)
{
	std::ostringstream stream;
	__print_control cfg;
	cfg.s = &stream;
	typedef int swallow[];
    (void) swallow {0,
            ( cfg.handle(args) )...
    };
    return stream.str();
}


/**
	Named objects are just used to enable human-readable reporting
  */
class named
{
	std::string n;
public:
	/// Make a name from a pointer
	static std::string anon(named const * ptr);

	named();
	named(const std::string& _n);

	inline void set_name(const std::string& _name) { n=_name; }
	inline std::string name() const { 
		if(n.empty()) 
			return anon(this);
		return n; 
	}
};


inline ostream& operator<<(ostream& s, const named& obj)
{
	return s << obj.name();
}


//------------------------------------------
//
// Type utilities
//
//------------------------------------------



/**
	Type-erased class for enumeration constant stringification
  */
class basic_enum_repr : public named
{
protected:
	std::map<int, std::string> extl;
	std::map<std::string, int> intl;
public:
	explicit basic_enum_repr(const std::string& ename) : named(ename) {}
	explicit basic_enum_repr(const std::type_info& ti);
	void add(int val, const std::string& tag);
	int map(const std::string& tag) const;
	std::string map(int val) const;
	bool is_member(int val) const;
	bool is_member(const std::string& tag) const;
};

/**
	Typed class for enumeration constant stringification
  */
template <typename Enum>
class enum_repr : public basic_enum_repr
{
public:
	typedef std::pair<Enum, const char*> value_type;
	explicit enum_repr( std::initializer_list< value_type > ilist ) 
	: basic_enum_repr(typeid(Enum)) 
	{
		for(auto&& e : ilist) {
			Enum val = std::get<0>(e);
			const std::string& tag = std::get<1>(e);
			add(static_cast<int>(val), tag);
		}
	}

	inline Enum operator[](const std::string& tag) const {
		return static_cast<Enum>(map(tag));
	}
	inline std::string operator[](Enum val) const {
		return map((int) val);
	}

};


}	// end namespace binc


#endif