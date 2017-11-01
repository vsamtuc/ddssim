
#include <boost/functional/hash.hpp>
#include <boost/polymorphic_pointer_cast.hpp>

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
	_warmup_size = none;
	_warmup_time = none;
	_cool = none;
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
void dataset::set_time_window(timestamp Tw) { _time_window = Tw; }

void dataset::warmup_size(size_t wsize, bool cool)
{
	using boost::none;
	_warmup_size = wsize;
	_warmup_time = none;
	_cool = cool;
}

void dataset::warmup_time(timestamp wtime, bool cool)
{
	using boost::none;
	_warmup_time = wtime;
	_warmup_size = none;
	_cool = cool;	
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
		src = time_window(src, _time_window.value());

	return src;
}


void dataset::create()
{
	using boost::none;

	if(!src) 
		throw std::runtime_error("no source");

	// if the source is not rewindable, we must materialize it
	if(_warmup_size != none)
		create_warmup_size(_warmup_size.value(), _cool.value());
	else if(_warmup_time != none)
		create_warmup_time(_warmup_time.value(), _cool.value());
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
			// analyze and rewind
			ds_metadata mdata = collect_metadata();
			// restore
			src = base_src;
			boost::polymorphic_pointer_cast<rewindable_data_source>(src)->rewind();
			apply_filters();
			src->set_metadata(mdata);
		} else {
			src = materialize(src);
			assert(0);
		}
	}
}


void dataset::create_warmup_size(size_t wsize, bool cool)
{
	/*
	apply_filters();
	if(! src->analyzed())
		src = materialize(src);
	*/
	create_no_warmup();
	assert(src->analyzed());

	// Check assertions
	if(wsize*(cool?2:1) >= src->metadata().size()) 
		throw std::runtime_error("requested warmup exhausts the data source");
	if(! (src->valid()))
		throw std::runtime_error("contrary to metadata, data source is exhausted");

	// load warmup into the buffer
	//CTX.warmup.clear();
	for(size_t i=0; i<wsize; i++) {
		//CTX.warmup.push_back(src->get());
		src->advance();
		if(! (src->valid()))
			throw std::runtime_error("unexpectedly, warmup exhausted the data source");
	}

	// do we need to cool?
	if(cool) {
		// the new length is equal to the current length minus wsize
		// note that max_length counts from first instantiation, ie. now
		size_t newlen = src->metadata().size()-wsize;
		src = filtered_ds(src, max_length(newlen));
	}

	// ok, now record metadata and rewind
	ds_metadata mdata = collect_metadata();
	src = base_src;
	boost::polymorphic_pointer_cast<rewindable_data_source>(src)->rewind();
	apply_filters();

	// set warmup
	CTX.warmup.clear();
	for(size_t i=0; i<wsize; i++) {
		CTX.warmup.push_back(src->get());
		src->advance();
	}

	// set cool
	if(cool) {
		// the new length is equal to the current length minus wsize
		// note that max_length counts from first instantiation, ie. now
		size_t newlen = mdata.size();
		src = filtered_ds(src, max_length(newlen));
	}

	src->set_metadata(mdata);
	src->set_warmup_size(wsize);
	src->set_cool_off(cool);

	//src = materialize(src);
	//src->set_warmup_size(wsize);
}

void dataset::create_warmup_time(timestamp wtime, bool cool)
{
	/*
	apply_filters();
	if(! src->analyzed())
		src = materialize(src);
	*/
	create_no_warmup();
	assert(src->analyzed());

	if(wtime*(cool?2:1) >= src->metadata().duration())
		throw std::runtime_error("requested warmup time exhausts the data source");
	if(! (src->valid()))
		throw std::runtime_error("contrary to metadata, data source is exhausted");
	if(src->get().ts != src->metadata().mintime())
		throw std::runtime_error("the metadata start time is not accurate");

	// load warmup into the buffer
	//CTX.warmup.clear();
	while( src->get().ts < src->metadata().mintime() + wtime ) {
		//CTX.warmup.push_back(src->get());
		src->advance();
		if(! (src->valid()))
			throw std::runtime_error("unexpectedly, warmup exhausted the data source");
	}

	// do we need to cool?
	if(cool) {
		// the new duration is equal to the current minus wtime
		size_t newend = src->metadata().maxtime() - wtime;
		src = filtered_ds(src, max_timestamp(newend));
	}

	//src = materialize(src);
	//src->set_warmup_time(wtime);
	// ok, now record metadata and rewind
	ds_metadata mdata = collect_metadata();
	src = base_src;
	boost::polymorphic_pointer_cast<rewindable_data_source>(src)->rewind();
	apply_filters();

	// load warmup into the buffer
	CTX.warmup.clear();
	while( src->get().ts < src->metadata().mintime() + wtime ) {
		CTX.warmup.push_back(src->get());
		src->advance();
	}

	if(cool) {
		// the new duration is equal to the current minus wtime
		size_t newend = mdata.maxtime();
		src = filtered_ds(src, max_timestamp(newend));
	}

	src->set_metadata(mdata);
	src->set_warmup_time(wtime);
	src->set_cool_off(cool);
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

