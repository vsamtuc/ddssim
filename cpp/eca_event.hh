#ifndef __ECA_EVENT_HH__
#define __ECA_EVENT_HH__

namespace dds {


/**
	The ECA event types
  */
class Event
{
	int id;
public:
	Event() : id(0) { }

	// used to construct new event ids, usually a global constants.
	constexpr Event(int _id): id(_id) { }

	constexpr inline bool operator==(Event evt) const {
		return id==evt.id;
	}
	constexpr inline bool operator<(Event evt) const {
		return id<evt.id;
	}

	constexpr inline operator int () const { return id; }
};


/*
	The system event types
 */


constexpr Event INIT(1);
constexpr Event DONE(2);
constexpr Event START_STREAM(3);
constexpr Event END_STREAM(4);
constexpr Event START_RECORD(5);
constexpr Event END_RECORD(6);
constexpr Event VALIDATE(7);
constexpr Event REPORT(8);


/*
	Event types for various modules.
	Making these constexpr allows them to be
	used in switch statements, etc
*/


// from accurate
constexpr Event STREAM_SKETCH_UPDATED(100);


}


// Extend the hash<> template in namespace std
namespace std {
template<> struct hash<dds::Event>
{
    typedef dds::Event argument_type;
    typedef std::size_t result_type;
    result_type operator()(argument_type s) const
    {
    	return hash<int>()(s);
    }
};
}


#endif