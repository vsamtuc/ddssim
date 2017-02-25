
#include <boost/functional/hash.hpp>

#include "method.hh"
//#include "mathlib.hh"

using namespace dds;

context dds::CTX;

basic_factory::~basic_factory()
{
	
}

output_file* context::open(FILE* f, bool owner) 
{
	output_file* of = new output_c_file(f, owner);
	result_files.push_back(of);
	return of;
}

output_file* context::open(const string& path, open_mode mode) 
{
	output_file* of = new output_c_file(path, mode);
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



dataset::dataset() 
: src(0) 
{
}

dataset::~dataset()
{ }

void dataset::clear() 
{ 
	src.reset();
}

void dataset::load(datasrc _src) 
{ 
	clear(); 
	src = _src; 
}
	
void dataset::set_max_length(size_t n) { _max_length = n; }
void dataset::hash_streams(stream_id h) { _streams = h; }
void dataset::hash_sources(source_id s) { _sources = s; }
void dataset::set_time_window(timestamp Tw) { _time_window = Tw; }

void dataset::create() {
	using boost::none;
	
	if(!src) {
		throw std::runtime_error("no source");
	}

	// apply filters
	if(_max_length != none) {
		auto ds = filtered_ds(src, max_length(_max_length.value()));
		assert(src.use_count()>1);
		src = ds;
	}
	if(_streams != none) {
		auto ds = filtered_ds(src, 
			modulo_attr(&dds_record::sid, _streams.value()));
		assert(src.use_count()>1);
		src = ds;
	}

	if(_sources != none) {
		auto ds = filtered_ds(src, 
			modulo_attr(&dds_record::hid, _sources.value()));
		assert(src.use_count()>1);
		src = ds;
	}

	// apply window
	if(_time_window != none)
		src = time_window(src, _time_window.value());

	CTX.data_feed(src);
	src.reset();
}


size_t dds::__hash_hashes(size_t* ptr, size_t n)
{
	using boost::hash_value;
	using boost::hash_combine;

	size_t seed = 0;
	while(n--) {
		hash_combine(seed, *ptr++);
	}
	return seed;
}

