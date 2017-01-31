
#include "method.hh"
#include "mathlib.hh"

using namespace dds;

executor::executor(data_source* _src) : src(_src) 
{}

void executor::run()
{
	if(! src->valid()) return;

	_now = src->get().ts;

	for(auto m : meths) {
		m->start(*this);
	}

	while(src->valid()) {
		const dds_record& rec = src->get();
		_now = rec.ts;
		for(auto m : meths) {
			m->process(rec);
		}
		src->advance();
	}

	for(auto m : meths) {
		m->finish();
	}
}

executor::~executor()
{
	for(auto m : meths) {
		delete m;
	}	

	if(src) delete src;
}



void estimating_method::process(const dds_record& rec)
{
	process_record(rec);
}


/*-------------------------
	Exact method
  -------------------------*/

struct exact_method::observer
{	
	estimating_method* method;
	estimate_error_observer error_observer;

	observer(estimating_method* m, size_t window)
	: method(m), error_observer(window) {}

	void observe(double exactest) {
		double est = method->current_estimate();
		error_observer.observe(est, exactest);
	}
};

void exact_method::start(executor& exc)
{
	// Populate observers
	for(size_t i=0; i<exc.methods(); i++) {

		// only previous methods are compared
		if(exc.get_method(i)==this) break;

		// not an estimating method
		estimating_method* m = 
			dynamic_cast<estimating_method*>(exc.get_method(i));
		if(m==nullptr) continue;

		// not for my query
		if(query()!=m->query()) continue;

		// ok, create an observer
		observers.push_back(new observer(m, window));		
	}
}


void exact_method::process(const dds_record& rec)
{
	// We assume that we are called here after
	// our estimate has been computed
	estimating_method::process(rec);
	double myest = current_estimate();
	for(auto o : observers) {
		o->observe(myest);
	}
}

void exact_method::finish()
{
	// Report final accuracy stats from all observers
}

exact_method::~exact_method()
{
	for(auto o : observers)
		delete o;
}
