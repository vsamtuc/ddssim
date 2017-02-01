#ifndef __METHOD_HH___
#define __METHOD_HH___

#include <vector> 
#include <map>
#include <deque>
#include <list>

#include "dds.hh"
#include "data_source.hh"

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
enum Event {
	INIT, DONE,
	START_STREAM, END_STREAM,
	START_RECORD, END_RECORD,
	VALIDATE, REPORT,

	// These are not callbacks, but are here for the
	// sake of the controller!
	__EMPTY
};

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
using eca_map = std::map<Event, action_seq>;

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
	The CTX context object's type
  */
struct context
{
	timestamp now;
	data_source* ds;

	event_queue_t event_queue;
	action_queue_t action_queue;
	eca_map rules;
	action* current_action;

	context() {}
	void run();

protected:
	void run_action(action*);
	void dispatch_event(Event);
public:
	template <typename T>
	inline eca_rule add_rule(Event evt, T* action) {
		action_seq& aseq = rules[evt];
		action_seq::iterator i = aseq.insert(aseq.end(), action);
		return std::make_pair(evt, i);
	}

	inline void cancel_rule(eca_rule rule) {
		action_seq& aseq = rules[rule.first];
		action_seq::iterator i = rule.second;
		delete (*i);
		aseq.erase(i);
	}
};

///  The global context
extern context CTX;

/// Used to add ECA rules
template <typename Action>
inline auto ON(Event evt, const Action& action)
{
	return CTX.add_rule(evt, new action_function<Action>(action));
}

/// Used to emit an event
inline void emit(Event evt)
{
	CTX.event_queue.push_back(evt);
}


/**
	Reactive objects manage a set of rules conveniently.

	Use the \c on() member to add ECA rules, that will be
	cancelled when the object is destroyed.

	\note this class (and its subclasses) are non-copyable
	and non-movable. 
  */
struct reactive
{
	std::vector<eca_rule> eca_rules;

	reactive() { }
	reactive(const reactive&) = delete;
	reactive& operator=(const reactive&) = delete;
	reactive(reactive&&) = delete;
	reactive& operator=(reactive&&) = delete;

	virtual ~reactive() {
		for(auto rule : eca_rules) CTX.cancel_rule(rule);	
	}

	template <typename Action>
	inline eca_rule on(Event evt, const Action& action) {
		eca_rule rule = ON(evt, action);
		eca_rules.push_back(rule);
		return rule;
	}
};


/**
	A module that manages the data source.

	Rules:
	- on INIT: 
	  - set up data source, 
	  - emit START_STREAM, 
	  - if the stream is valid emit START_RECORD, else
	    emit END_STREAM

	- on END_RECORD
	  - advance the stream
	  - if the stream is valid emit START_RECORD, else
	    emit END_STREAM
*/
struct data_feeder : reactive
{
	data_feeder(data_source* src);
	void advance();
	void proceed();
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
struct basic_control : reactive
{
private:
	enum State { NoData, Data, Validate, Report };
	State state = NoData;
public:
	basic_control() {
		on(__EMPTY, [&](){ handler(); });
		on(START_STREAM, [&](){ state=Data; });
		on(END_STREAM, [&](){ state=NoData; });
	}
	void handler()
	{
		switch(state) {
			case Data:
				emit(VALIDATE);
				state = Validate;
				break;
			case Validate:
				emit(REPORT);
				state = Report;
				break;
			case Report:
				emit(END_RECORD);
				state = Data;
				break;
			default:
				emit(DONE);
		}
	}
};


/**
	A protocol is a simulation of a query answering method.

	This is the base class.
  */
class protocol : public reactive
{
public:
	virtual const basic_query& query() const = 0;
	virtual double current_estimate() const = 0;
};




} //end namespace dds

#endif
