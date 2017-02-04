#ifndef __ECA_HH__
#define __ECA_HH__


#include <vector> 
#include <map>
#include <unordered_map>
#include <deque>
#include <list>

#include "dds.hh"
#include "data_source.hh"
#include "output.hh"

namespace dds {

/*
	An event-based execution model.

	A simulation is composed from a number of independent
	objects (components), which are synchronized via an
	event-condition-action mechanism.

	A data source is encapsulated by a \c data_feeder.
	Other components include:
	- simulated protocols for query answering
	- report modules, managing data output
	- validation modules, contrasting modules with each
	  other.
	- simulation execution adaptors, etc.

	During a simulation, all components share the global
	CTX object, which is used to hold common information
	- ECA rule runtime (queues)
	- pointer to data source
	- pointers to config objects
	- pointers to data output objects
	- etc
 */


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

// back to dds
namespace dds {


constexpr Event INIT(1);
constexpr Event DONE(2);
constexpr Event START_STREAM(3);
constexpr Event END_STREAM(4);
constexpr Event START_RECORD(5);
constexpr Event END_RECORD(6);
constexpr Event VALIDATE(7);
constexpr Event REPORT(8);


using event_queue_t = std::deque<Event>;

/**
	Actions of ECA rules.
  */
struct action
{
	virtual void run() = 0;
	virtual ~action() { }
};

using action_queue_t = std::deque<action*>;
using action_seq = std::list<action*>;
using eca_rule = std::pair<Event,action_seq::iterator>;
using eca_map = std::unordered_map<Event, action_seq>;

/**
	Typed wrapper for Actions
  */
template <typename Action>
struct action_function : action
{
	Action action_func;
	action_function(const Action& _action) 
	: action_func(_action) { }
	virtual void run() override {
		action_func();
	}
};


/**
	Scheduled actions at a subset of the steps
  */
template <typename Condition, typename Action>
struct condition_action : action_function<Action>
{
	Condition condition_func;
	condition_action(const Condition& c, const Action& a) 
	: action_function<Action>(a), condition_func(c)
	{ }

	virtual void run() {
		if(condition_func()) this->action_func();
	}
};


/* 
	Some useful condition objects 
 */
struct every_n_times
{
	size_t N;
	size_t count;
	every_n_times(size_t _N) : N(_N), count(_N) { 
		assert(_N>0);
	}

	bool operator()() {
		if((--count)==0) {
			count = N;
			return true;
		} else 
			return false;
	}
};



/**
	A basic controller for executions.

	A controller is a stateful object that drives the
	simulation when needed. A controller reacts to
	the situation where there are no further actions
	enqueued, but the DONE event has not been emitted.
	This is achieved by capturing the special __EMPTY
	event.

 */
struct basic_control
{
protected:

	// State for ECA callbacks

	event_queue_t event_queue; // the emitted but not dispatched events
	action_queue_t action_queue; // the dispatched actions
	eca_map rules;  // the rules
	action* current_action;  // the current action, or null
	bool purge_current = false; // denote that the current action 
							// is to be purged asap (or null)

	void purge_action(action* a);

	// used for an invalid data source
	struct __invalid_data_source : analyzed_data_source {
		__invalid_data_source() { isvalid = false; }
	};
	static __invalid_data_source __invds;

	// current time
	timestamp _now;

	// data source
	analyzed_data_source* ds;

	// internal methods
	void run_action(action*);
	void dispatch_event(Event);
	void empty_handler();
	void advance();
	void proceed();


public:

	enum State { 
		Start,
		Init, Data, Validate, Report, EndData, 
		End
	};

protected:
	State state = Start;

public:
	inline State get_state() const { return state; }

	inline timestamp now() const { return _now; }

	inline const dds_record& stream_record() const { return ds->get(); }

	inline const ds_metadata& metadata() const {
		return ds->metadata();
	}

	/**
		Add data source to the controller
	  */
	void data_feed(data_source* src);

	/**
		Run the controller
	  */
	void run();

	/** 
		Add a new ECA rule.
		This is done by passing the action object pointer
		directly. NOTE: The object will still be destroyed
		when the rule is cancelled, i.e. the context takes
		ownership.
	 */
	inline eca_rule add_rule(Event evt, action* _action) {
		action_seq& aseq = rules[evt];
		action_seq::iterator i = aseq.insert(aseq.end(), _action);
		return std::make_pair(evt, i);
	}

	/**
		Cancel an ECA rule.
	  */
	void cancel_rule(eca_rule rule);

	/// Used to add ECA rules
	template <typename Action>
	inline auto on(Event evt, const Action& action)
	{
		return add_rule(evt, new action_function<Action>(action));
	}

	template <typename Condition, typename Action>
	inline auto on(Event evt, const Condition& cond, const Action& action)
	{
		return add_rule(evt, 
			new condition_action<Condition, Action>(cond, action));
	}

	/// Used to emit an event
	inline void emit(Event evt)
	{
		event_queue.push_back(evt);
	}

	basic_control() : ds(&__invds)
	{ }
};





} // end namespace dds

#endif