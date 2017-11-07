
#include <boost/functional/hash.hpp>
#include <boost/polymorphic_pointer_cast.hpp>

#include "method.hh"
//#include "mathlib.hh"

using namespace dds;

context dds::CTX;

basic_factory::~basic_factory()
{
	
}

output_file* context::open(FILE* f, bool owner, text_format fmt) 
{
	output_file* of = new output_c_file(f, owner, fmt);
	result_files.push_back(of);
	return of;
}

output_file* context::open(const string& path, open_mode mode, text_format fmt) 
{
	output_file* of = new output_c_file(path, mode, fmt);
	result_files.push_back(of);
	return of;
}

output_file* context::open_hdf5(const string& path, open_mode mode) 
{
	output_file* of = new output_hdf5(path, mode);
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

void context::clear()
{
}

void context::run()
{
	basic_control::run();
}



dataset::dataset() 
: base_src(0), src(0) 
{
}

dataset::~dataset()
{ }

void dataset::clear() 
{ 
	using boost::none;

	src.reset();
	base_src.reset();
	_name = none;
	_max_length = none;
	_streams = none;
	_sources = none;
	_time_window = none;
	_wflush = true;
	_warmup_size = none;
	_warmup_time = none;
}

void dataset::load(datasrc _src) 
{ 
	src.reset();
	base_src.reset();
	base_src = src = _src;
}


void dataset::set_name(const string& n) { _name = n; }
void dataset::set_max_length(size_t n) { _max_length = n; }
void dataset::hash_streams(stream_id h) { _streams = h; }
void dataset::hash_sources(source_id s) { _sources = s; }

void dataset::set_time_window(timestamp Tw, bool flush) 
{ 
	using boost::none;
	_time_window = Tw; _wflush = flush; 
	_fixed_window = none;
}

void dataset::set_fixed_window(size_t W, bool flush) 
{ 
	using boost::none;
	_fixed_window = W; _wflush = flush; 
	_time_window = none;
}

void dataset::warmup_size(size_t wsize)
{
	using boost::none;
	_warmup_size = wsize;
	_warmup_time = none;
}

void dataset::warmup_time(timestamp wtime)
{
	using boost::none;
	_warmup_time = wtime;
	_warmup_size = none;
}


datasrc dataset::apply_filters()
{
	using boost::none;
	
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
		src = time_window(src, _time_window.value(), _wflush);
	if(_fixed_window != none)
		src = fixed_window(src, _fixed_window.value(), _wflush);

	return src;
}


void dataset::create()
{
	using boost::none;

	if(!src) 
		throw std::runtime_error("no source");

	// if the source is not rewindable, we must materialize it
	if(_warmup_size != none)
		create_warmup_size(_warmup_size.value());
	else if(_warmup_time != none)
		create_warmup_time(_warmup_time.value());
	else
		create_no_warmup();

	if(_name != none)
		src->set_name(_name.value());
	CTX.data_feed(src);
	src.reset();
	base_src.reset();
}

ds_metadata dataset::collect_metadata()
{
	ds_metadata mdata = src->metadata();
	mdata.prepare_collect();
	for(auto rec : *src)
		mdata.collect(rec);
	mdata.set_valid();
	return mdata;
}

void dataset::create_no_warmup() 
{
	apply_filters();
	if(! src->analyzed()) {
		if(src->rewindable()) {
			ds_metadata mdata = collect_metadata();
			src->rewind();
			src->set_metadata(mdata);
		} else {
			src = materialize(src);
			assert(0);
		}
	}
}


void dataset::create_warmup_size(size_t wsize)
{
	create_no_warmup();
	CTX.warmup.clear();
	src->warmup_size(wsize, & CTX.warmup);
}

void dataset::create_warmup_time(timestamp wtime)
{
	create_no_warmup();
	CTX.warmup.clear();
	src->warmup_time(wtime, & CTX.warmup);
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

