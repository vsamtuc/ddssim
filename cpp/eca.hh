#ifndef __ECA_HH__
#define __ECA_HH__


#include <map>
#include <unordered_map>
#include <deque>
#include <list>

#include "dds.hh"
#include "data_source.hh"
#include "output.hh"
#include "eca_event.hh"

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


	New event types can be added in eca_event.hh
 */


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
 *
 *	Some useful condition objects 
 *
 */

/**
	A function object that returns true once every \f$n\f$ times.

	For example, `every_n_times(3)` is a function object that will 
	return the following sequence (in binary): `001001001001001....`.
  */
struct every_n_times
{
	/// The object's period
	size_t n;

	/// the current state
	size_t t;

	every_n_times(size_t _n);
	bool operator()();
};

/**
	A function object that returns true \f$n\f$ times out of \f$N\f$.

	In particular, for \f$N\geq n>1\f$, this object will return true
	exactly \f$n\f$ times, more or less evenly spaced with \f$N/(n-1)\f$ calls
	between them.
	Note that the first call and the \f$N\f$-th call will always be true.

	If initialized with \f$n\geq N>0\f$, the object will return true on every
	call. 

	If initialized with \f$N\geq n=1\f$, the object will return true only the
	first time.
  */
struct n_times_out_of_N
{
	size_t N;  ///< The length of the interval
	size_t n;  ///< The number of true times

	// state
	size_t t;		// time of next call
	size_t tnext;	// time of next true call
	size_t r;			// remaining true calls

	n_times_out_of_N(size_t _n, size_t _N);
	bool operator()();
};


/**
	Return a predicate that will be true `n` times during a run.

	The data source of the context must be set, or an
	`std::out_of_range` exception is thrown.
  */
n_times_out_of_N n_times(size_t n);


/**
	A predicate object which returns true when a level change
	occurs.

	Given a function object `func` and two weights, `p` and `d`,
	the predicate returns true on every call, where
	```
		fabs( func() - f_last ) > p*fabs(f_last) + d
	```

	Note: by setting `d==0.0` we can watch for relative changes, 
	and setting `p==0.0` we can watch for absolute changes.

	Performance: the function `func` is called once at each invocation
	of the predicate.
  */
struct level_changed
{
	typedef std::function<double ()> real_func;
	real_func func; 				// the value provider
	double p,d;						// the level check

	// state: last value
	double f_last;

	/// Basic constructor
	level_changed(const real_func& _f, double _p, double _d, double f_init);

	/** 
		Constructor.

		The initial value `f_init` is computed by calling `_f()` 
	  */
	level_changed(const real_func& _f, double _p, double _d);

	bool operator()();
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


	// current time
	timestamp _now;

	// data source
	datasrc ds;

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
		Results,
		End
	};

protected:
	State state = Start;

	size_t _step;
	size_t _recno;
public:
	inline State get_state() const { return state; }

	inline timestamp now() const { return _now; }

	inline size_t step() const { return _step; }

	inline const dds_record& stream_record() const { return ds->get(); }

	inline size_t stream_count() const { return _recno; }

	inline const ds_metadata& metadata() const {
		return ds->metadata();
	}

	/**
		Add data source to the controller
	  */
	void data_feed(datasrc src);

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

	basic_control();

	~basic_control();
};





} // end namespace dds

#endif