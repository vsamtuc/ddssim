
#include <cstdio>
#include <algorithm>
#include "method.hh"
#include "output.hh"

using namespace dds;


basic_column::~basic_column()
{
	if(_table) {
		_table->columns[_index] = 0;
		_table->set_enabled(false);
	}
}



__binding::__binding(output_file* f, output_table* t)
: file(f), table(t), 
	in_file_list(f->tables.insert(f->tables.end(),this)),
	in_table_list(t->files.insert(t->files.end(), this)),
	enabled(true)
{ }

void __binding::unbind_all(__binding::list& L)
{
	while(! L.empty()) {
		__binding* b = L.front();
		delete b;
	}
}

__binding::~__binding()
{
	file->tables.erase(in_file_list);
	table->files.erase(in_table_list);
}


__binding* __binding::find(list& L, output_file* f)
{
	auto found = std::find_if(L.begin(), L.end(), 
		[=](__binding* b){ return b->file==f; });
	if(found==L.end())
		return nullptr;
	else
		return *found;
}

__binding* __binding::find(list& L, output_table* t)
{
	auto found = std::find_if(L.begin(), L.end(), 
		[=](__binding* b){ return b->table==t; });
	if(found==L.end())
		return nullptr;
	else
		return *found;
}

output_table::output_table(const string& _name, table_flavor _f)
: named(_name), en(true), _flavor(_f)
{ 
}


output_table::~output_table()
{
	for(auto c : columns) {
		// dissociate with column
		if(c) c->_table = 0;
	}
	__binding::unbind_all(files);
}


void output_table::emit_row()
{
	for(auto b : bindings())
		b->file->output_row(*this);
}

void output_table::prolog()
{
	for(auto b : bindings())
		b->file->output_prolog(*this);
}

void output_table::epilog()
{
	for(auto b : bindings())
		b->file->output_epilog(*this);
}

result_table::result_table(const string& _name)
	: output_table(_name, table_flavor::RESULTS)
{}

result_table::result_table(const string& _name,
		column_list col)
	: output_table(_name, table_flavor::RESULTS)
{
	add(col);
}

void result_table::add(column_list col)
{
	for(auto c : col) this->output_table::add(*c);	
}

result_table::~result_table()
{
}


time_series::time_series(const string& _name) 
: output_table(_name, table_flavor::TIMESERIES), 
	now("time", "%d") 
{
	add(now);
}

time_series::time_series() 
: time_series("timeseries") 
{}



output_file::output_file()
{ }
output_file::~output_file()
{ }


void output_c_file::open(const string& fpath, open_mode mode)
{
	if(stream) 
		throw std::runtime_error("output file already open");
	stream = fopen(fpath.c_str(), (mode==open_mode::append?"a":"w"));
	if(!stream) 
		throw std::runtime_error("I/O error opening file");
	filepath = fpath;
	owner = true;
}

void output_c_file::close()
{
	if((!stream)) return;
	if(owner) {
		if(fclose(stream)!=0)
			throw std::runtime_error("I/O error closing file");		
	} else {
		flush();
	}
	stream = nullptr;
	owner = false;
	filepath = string();
}

void output_c_file::flush()
{
	if(!stream)
		throw std::runtime_error("I/O error flushing closed file");
	if(fflush(stream)!=0)
		throw std::runtime_error("I/O error flushing file");
}


output_c_file::output_c_file(FILE* _stream, bool _owner)
: stream(_stream), owner(_owner) { }


output_c_file::output_c_file(const string& _fpath, open_mode mode)
: output_c_file()
{
	open(_fpath, mode);
}


output_c_file::~output_c_file()
{
	__binding::unbind_all(tables);
	close();
}


void output_c_file::emit(basic_column* col)
{
	// Normally, this does not belong to the column!!!
	col->emit(file());
}

void output_c_file::output_prolog(output_table& table)
{
	table_flavor flavor = table.flavor();

	switch(flavor) {
	case table_flavor::RESULTS:
	case table_flavor::TIMESERIES:
		break;
	default:
		throw std::runtime_error("incompatible flavor");
	}

	for(size_t col=0;col < table.size(); col++) {
		if(col) fputs(",", file());
		fputs(table[col]->name().c_str(), file());
	}
	fputs("\n", file());	
}

void output_c_file::output_row(output_table& table)
{
	table_flavor flavor = table.flavor();

	switch(flavor) {
	case table_flavor::RESULTS:
	case table_flavor::TIMESERIES:
		break;
	default:
		throw std::runtime_error("incompatible flavor");
	}

	for(size_t col=0;col < table.size(); col++) {
		if(col) fputs(",", file());
		emit(table[col]);
	}
	fputs("\n", file());
}

void output_c_file::output_epilog(output_table&)
{ }


output_c_file dds::output_stdout(stdout, false);
output_c_file dds::output_stderr(stderr, false);


progress_bar::progress_bar(FILE* s, size_t b, const string& msg)
        : stream(s), message(msg), 
        N(0), i(0), ni(0), B(b), l(0), finished(false)
        { }

void progress_bar::start(unsigned long long _N)
{	
	N = _N;
	i=0; ni=0; l=0;

    ni = nexti();
    size_t spc = B+1+message.size();
    for(size_t j=0; j< spc; j++) putchar(' ');
    fprintf(stream, "]\r%s[", message.c_str());
    fflush(stream);
    tick(0);
}

void progress_bar::adjustBar()
{
    if(i>N) i=N;
    while(i >= ni)  {
        ++l; ni = nexti();
        if(l<=B) { fputc('#', stream); }
    }
    fflush(stream);
    if(l==B) {
    	fprintf(stream,"\n");
        finished = true;
    }
}

void progress_bar::finish()
{
    if(finished) return;
    if(i<N)
	    tick(N-i);
}


//-------------------------------------
//
// HDF5 files
//
//-------------------------------------


