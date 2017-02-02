
#include "method.hh"
#include "mathlib.hh"

using namespace dds;

context dds::CTX;



output_file* context::open(FILE* f, bool owner) 
{
	output_file* of = new output_file(f, owner);
	result_files.push_back(of);
	return of;
}

output_file* context::open(const string& path, open_mode mode) 
{
	output_file* of = new output_file(path, mode);
	result_files.push_back(of);
	return of;
}

void context::close_result_files()
{
	for(auto f : result_files) {
		delete f;
	}
	result_files.clear();
}


void context::run()
{
	basic_control::run();
}
