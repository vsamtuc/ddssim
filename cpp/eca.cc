
#include "dds.hh"
#include "eca.hh"

#define ECA_TRACE


using namespace dds;

using namespace std::literals;

void basic_control::empty_handler()
{

	switch(state) {
		case Start:
			emit(INIT);
			state = Init;
			break;
		case Init:
			if(ds) {
				emit(START_STREAM);
				state = Data;
			} else {
				emit(DONE);
				state = End;
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
			emit(DONE);
			state = End;
			break;
		case End:
			assert(0);
	}
}


void basic_control::run_action(action* a)
{
	current_action = a;
	a->run();
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
	while(state != End) {

		if(!action_queue.empty()) {

			action* a = action_queue.front();
			action_queue.pop_front();
			run_action(a);

		} else if(! event_queue.empty()) {

			Event evt = event_queue.front();
			event_queue.pop_front();
			dispatch_event(evt);

		} else {

			empty_handler();

		}

	}

}


basic_control::__invalid_data_source basic_control::__invds;

void basic_control::data_feed(data_source* src)
{
	// delete current ds
	if(src==nullptr) {
		if(ds!=&__invds) delete ds;
		ds = &__invds;
		return;
	}

	// invalidate current ds
	data_feed(nullptr);

	// set current ds
	ds = dynamic_cast<analyzed_data_source*>(src);

	// analyze if needed
	if(!ds) {
		// not an analyzed data source, analyze
		ds = new materialized_data_source(src);
	}
}


