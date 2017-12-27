#include <cassert>
#include <algorithm>
#include <cmath>

#include "eca.hh"

#define ECA_TRACE

using namespace eca;

void engine::cancel_rule(eca_rule rule)
{
	// Rule cancelation is complicated by the fact
	// that we need to delete the action
	// BUT!!! The action may be already in the
	// action queue, or worse, may be the current
	// action!!!
	action_seq& aseq = rules[rule.first];
	action_seq::iterator i = rule.second;

	if( (*i) == current_action) {
		// if (*i) is the current action, we
		// cannot remove it!
		assert(! purge_current);
		purge_current = true;
	} else {
		purge_action(*i);
	}

	aseq.erase(i);
}

void engine::purge_action(action* a)
{
	// remove from the action queue ( O(n) time ...)
	auto pos = std::remove(action_queue.begin(), action_queue.end(), a);
	auto num_instances = distance(pos, action_queue.end());
	action_queue.resize(action_queue.size()-num_instances);
	delete a;
}



void engine::run_action(action* a)
{
	_step++;
	current_action = a;
	a->run();
	if(purge_current) {
		purge_action(current_action);
		purge_current = false;
	}
	current_action = nullptr;
}



void engine::dispatch_event(Event evt)
{
	// Actions for control and data handling
	auto aseq = rules[evt];
	std::copy(aseq.begin(), aseq.end(), back_inserter(action_queue));
}


void engine::run()
{
	while(true) {

		if(!action_queue.empty()) {

			action* a = action_queue.front();
			action_queue.pop_front();
			run_action(a);

		} else if(! event_queue.empty()) {

			Event evt = event_queue.front();
			event_queue.pop_front();
			dispatch_event(evt);

		} else if(! event_stack.empty()) {

			Event evt = event_stack.back();
			event_stack.pop_back();
			dispatch_event(evt);			

		} else {
			break;
		}

	}
}


void engine::initialize()
{
	event_queue.clear();
	action_queue.clear();
	current_action = nullptr;
	purge_current = false;

	_step = 0;
}



engine::engine()
{
}


engine::~engine()
{ }


//-----------------------------------------
//
//  Condition objects
//
//-----------------------------------------


every_n_times::every_n_times(size_t _n) 
: n(_n), t(_n) 
{ 
	assert(_n>0);
}

bool every_n_times::operator()() 
{
	if((--t)==0) {
		t = n;
		return true;
	} else 
		return false;
}


n_times_out_of_N::n_times_out_of_N(size_t _n, size_t _N) 
: N(_N), n(std::min(_n, _N)), t(0), tnext(0), r(n)
{
	if(N==0) throw std::out_of_range("the period cannot be 0");
	if(n==0) tnext = N;
}


bool n_times_out_of_N::operator()() 
{
	bool ret = (t==tnext);

	// update state
	t++; 
	if(t==N) {
		// last call, reset
		r = n; t=0; tnext=0;
	} else if(ret) {
		r--;
		tnext = (r>0) ? 
			t-1+(N-t)/r   /* remaining calls / remaining true calls */
			: N /*never!*/ ;
	}

	return ret;
}




level_changed::level_changed(const real_func& _f, double _p, double _d, 
	double f_init)
: func(_f), p(_p), d(_d), f_last(f_init)
{ }

level_changed::level_changed(const real_func& _f, double _p, double _d)
: level_changed(_f, _p, _d, _f())
{ }

bool level_changed::operator()()
{
	double f_cur = func();
	if( fabs(f_cur - f_last) > p*f_last + d ) {
		f_last = fabs(f_cur);
		return true;
	}
	return false;
}

