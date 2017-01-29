
#include "method.hh"

using namespace dds;

executor::executor(data_source* _src) : src(_src) 
{}

void executor::run()
{
	for(auto m : methods) {
		m->start();
	}

	while(src->valid()) {
		for(auto m : methods) {
			const dds_record& rec = src->get();
			m->process(rec);
		}
		src->advance();
	}

	for(auto m : methods) {
		m->finish();
	}
}

executor::~executor()
{
	for(auto m : methods) {
		delete m;
	}	

	if(src) delete src;
}