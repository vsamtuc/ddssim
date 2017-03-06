
#include "dds.hh"
#include "eca.hh"
#include "method.hh"

#define ECA_TRACE


using namespace dds;

//using namespace std::literals;


void basic_control::cancel_rule(eca_rule rule)
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

void basic_control::purge_action(action* a)
{
	// remove from the action queue ( O(n) time ...)
	auto pos = remove(action_queue.begin(), action_queue.end(), a);
	auto num_instances = distance(pos, action_queue.end());
	action_queue.resize(action_queue.size()-num_instances);
	delete a;
}


void basic_control::empty_handler()
{

	switch(state) {
		case Start:
			_step = 0;
			emit(INIT);
			state = Init;
			break;
		case Init:
			if(ds) {
				emit(START_STREAM);
				state = Data;
			} else {
				emit(RESULTS);
				state = Results;
			}
			break;
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
		case EndData:
			emit(RESULTS);
			state = Results;

			break;
		case Results:
			emit(DONE);
			state = End;
			break;			
		case End:
			assert(0);
	}
}


void basic_control::run_action(action* a)
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


void basic_control::advance()
{
	assert(ds->valid());

	ds->advance();
	proceed();
}

void basic_control::proceed()
{
	if(ds->valid()) {
		// set the time!
		_now = ds->get().ts;
		emit(START_RECORD);
	} else {
		emit(END_STREAM);
	}	
}


void basic_control::dispatch_event(Event evt)
{
	// Actions for control and data handling
	switch(evt) {
		case START_STREAM:
			proceed();				
			break;

		case START_RECORD:
			_recno++;
			break;

		case END_RECORD:
			advance();
			break;

		case END_STREAM:
			state = EndData;
			break;

		default:
			break;
	}

	auto aseq = rules[evt];
	std::copy(aseq.begin(), aseq.end(), back_inserter(action_queue));
}


void basic_control::run()
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

		} else if(state != End) {
			empty_handler();			
		} else break;

	}

}

// used for an invalid data source
namespace {
	struct __invalid_data_source : data_source {
		__invalid_data_source() { isvalid = false; }
	};
}

void basic_control::data_feed(datasrc src)
{
	// delete current ds
	if(!src) {
		ds = datasrc(new __invalid_data_source());
		return;
	}

	// analyze if needed
	if(!src->analyzed()) {
		// not an analyzed data source, analyze
		ds = datasrc(new materialized_data_source(src));
	} else {
		ds = src;
	}
}

basic_control::basic_control()
{
	data_feed(nullptr);
}


basic_control::~basic_control()
{ }


//
//  Condition objects
//


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


n_times_out_of_N dds::n_times(size_t n)
{
	return n_times_out_of_N(n, CTX.metadata().size());
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

