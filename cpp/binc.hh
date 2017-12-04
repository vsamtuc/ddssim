#ifndef __BINC_HH__
#define __BINC_HH__

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



}	// end namespace binc


#endif