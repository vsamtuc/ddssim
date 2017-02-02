
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


output_table::output_table(const char* _name, 
	std::initializer_list<basic_column *> col)
: named(_name), en(true), columns(col)
{ 
	for(size_t i=0; i<columns.size(); ++i) {
		__associate(columns[i],i);
	}
}


output_table::~output_table()
{
	for(auto c : columns) {
		if(c) c->_table = 0;
	}
}

void output_table::emit_header_start(FILE* stream)
{
	fputs("#INDEX",stream);
}

void output_table::emit_row_start(FILE* stream)
{
	fprintf(stream,"%s", name().c_str());
}

void time_series::emit_header_start(FILE* stream)
{
	fprintf(stream, "%s", now.name().c_str());
}

void time_series::emit_row_start(FILE* stream)
{
	fprintf(stream,now.format(), now.value());
}

void time_series::emit_row()
{
	now = CTX.now;
	result_table::emit_row();
}

void output_table::emit_header(FILE* stream)
{
	if(!enabled()) return;
	emit_header_start(stream);
	for(auto col : columns) {
		fprintf(stream, ",%s", col->name().c_str());
	}
	fputs("\n",stream);
}

void output_table::emit(FILE* stream)
{
	if(!enabled()) return;
	emit_row_start(stream);
	for(auto col : columns) {
		fputs(",", stream);
		col->emit(stream);
	}
	fputs("\n",stream);
}


void result_table::emit_row()
{
	if(! enabled()) return;
	for(auto b : files) {
		if(b->enabled)
			emit(b->file->file());
	}
}

void result_table::emit_header_row()
{
	if(! enabled()) return;
	for(auto b : files) {
		if(b->enabled)
			output_table::emit_header(b->file->file());
	}
}


result_table::~result_table()
{
	__binding::unbind_all(files);
}

void output_file::open(const string& fpath, open_mode mode)
{
	if(stream) 
		throw std::runtime_error("output file already open");
	stream = fopen(fpath.c_str(), (mode==append?"a":"w"));
	if(!stream) 
		throw std::runtime_error("I/O error opening file");
	filepath = fpath;
	owner = true;
}

void output_file::close()
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

void output_file::flush()
{
	if(!stream)
		throw std::runtime_error("I/O error flushing closed file");
	if(fflush(stream)!=0)
		throw std::runtime_error("I/O error flushing file");
}


output_file::output_file(FILE* _stream, bool _owner)
: stream(_stream), owner(_owner) { }


output_file::output_file(const string& _fpath, open_mode mode)
: output_file()
{
	open(_fpath, mode);
}


output_file::~output_file()
{
	__binding::unbind_all(tables);
	close();
}


__binding::__binding(output_file* f, result_table* t)
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

__binding* __binding::find(list& L, result_table* t)
{
	auto found = std::find_if(L.begin(), L.end(), 
		[=](__binding* b){ return b->table==t; });
	if(found==L.end())
		return nullptr;
	else
		return *found;
}


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


