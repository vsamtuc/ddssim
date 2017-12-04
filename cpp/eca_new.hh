#ifndef __ECA_NEW_HH__
#define __ECA_NEW_HH__

#include <iostream>


using std::ostream;

namespace eca {


template <typename Arg>
struct event;

template <typename Arg>
struct event_type
{
	const char* const name;

	explicit constexpr event_type(const char* _name) : name(_name) { }

	event_type(event_type<Arg>&) = delete;
	event_type(event_type<Arg>&&) = delete;

	inline event<Arg> operator()(const Arg& arg) const {
		return event<Arg> { this, arg };
	}
};


template <typename Arg>
struct event
{
	const event_type<Arg>* const id;
	Arg arg;

	inline bool operator==(const event_type<Arg>& other) const {
		return id==other.id && arg==other.arg;
	}
};



} //end namespace eca


template<typename Arg>
inline ostream& operator<<(ostream &s, const eca::event_type<Arg>& etype)
{
	return s << etype.name;
}

template<typename Arg>
inline ostream& operator<<(ostream &s, eca::event<Arg> evt)
{
	return s << evt.id->name << "(" << evt.arg << ")" ;
}



#endif