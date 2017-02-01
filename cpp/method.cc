
#include "method.hh"
#include "mathlib.hh"

using namespace dds;

context dds::CTX;

void context::run_action(action* a)
{
	current_action = a;
	a->run();
	current_action = nullptr;
}

void context::dispatch_event(Event evt)
{
	auto aseq = rules[evt];
	std::copy(aseq.begin(), aseq.end(), back_inserter(action_queue));	
}

void context::run()
{
	bool done = false;
	while(true) {
		if(!action_queue.empty()) {
			action* a = action_queue.front();
			action_queue.pop_front();
			run_action(a);
		} else if((!done) && (! event_queue.empty())) {
			Event evt = event_queue.front();
			event_queue.pop_front();
			if(evt==DONE) done=true;
			dispatch_event(evt);
		} else if(! done) {
			dispatch_event(__EMPTY);
			if(action_queue.empty()) {
				done=true;
				dispatch_event(DONE);
			}	
		} else 
			break;
	}
}

data_feeder::data_feeder(data_source* src)
{
	on(END_RECORD, [=](){ advance(); });
	on(INIT, [=](){ 
		CTX.ds = src;
		emit(START_STREAM);
		proceed();
	});

}

void data_feeder::advance()
{
	assert(CTX.ds->valid());

	CTX.ds->advance();
	proceed();
}

void data_feeder::proceed()
{
	if(CTX.ds->valid()) {
		emit(START_RECORD);
	} else {
		emit(END_STREAM);
	}	
}


