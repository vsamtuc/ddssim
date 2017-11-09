
#include <cstdio>
#include <cstddef>
#include <algorithm>

#include "method.hh"
#include "output.hh"
#include "hdf5_util.hh"
#include "binc.hh"

using namespace dds;


//-------------------------------------
//
// bindings
//
//-------------------------------------

output_binding::output_binding(output_file* f, output_table* t)
: file(f), table(t), 
	in_file_list(f->tables.insert(f->tables.end(),this)),
	in_table_list(t->files.insert(t->files.end(), this)),
	enabled(true)
{ }

void output_binding::unbind_all(output_binding::list& L)
{
	while(! L.empty()) {
		output_binding* b = L.front();
		delete b;
	}
}

output_binding::~output_binding()
{
	file->tables.erase(in_file_list);
	table->files.erase(in_table_list);
}


output_binding* output_binding::find(list& L, output_file* f)
{
	auto found = std::find_if(L.begin(), L.end(), 
		[=](output_binding* b){ return b->file==f; });
	if(found==L.end())
		return nullptr;
	else
		return *found;
}

output_binding* output_binding::find(list& L, output_table* t)
{
	auto found = std::find_if(L.begin(), L.end(), 
		[=](output_binding* b){ return b->table==t; });
	if(found==L.end())
		return nullptr;
	else
		return *found;
}


//-------------------------------------
//
// basic_column
//
//-------------------------------------


basic_column::basic_column(output_table* _tab, const string& _name, 
	const string& f, 
	const type_info& _t, size_t _s, size_t _a)
: named(_name), _table(nullptr), 
	_format(f), 
	_type(_t), _size(_s), _align(_a)
 { 
 	if(_tab) _tab->add(*this);
 }


basic_column::~basic_column()
{
	if(_table) {
		_table->remove(*this);
	}
}

void basic_column::set(double val)
{
	using namespace std::string_literals;
	throw std::invalid_argument("wrong column type: "s+name()
		+" is not arithmetic");
}

void basic_column::set(const string&)
{
	using namespace std::string_literals;
	throw std::invalid_argument("wrong column type"+name()
		+" is not textual");
}



//-------------------------------------
//
// Tables (result_table + timeseries)
//
//-------------------------------------

static std::unordered_map<string, output_table*> __table_registry;
static std::unordered_set<output_table*> __all_tables;

output_table* output_table::get(const string& name)
{
	auto iter = __table_registry.find(name);
	return (iter!=__table_registry.end()) ? iter->second : nullptr;
}


const std::unordered_set<output_table*> all()
{
	return __all_tables;
}


output_table::output_table(const string& _name, table_flavor _f)
: named(_name), en(true), _locked(false), _dirty(false), _flavor(_f)
{
	if(_name.empty())
		throw std::runtime_error("Table cannot have empty name");
	if(__table_registry.count(_name)>0)
		throw std::runtime_error("A table of name `"+_name+"' is already registered");
	__table_registry[_name] = this;
	__all_tables.insert(this);	
}


output_table::~output_table()
{
	for(auto c : columns) {
		// dissociate with column
		if(c) c->_table = 0;
	}
	output_binding::unbind_all(files);
	__table_registry.erase(this->name());
	__all_tables.erase(this);
}


void output_table::_cleanup()
{
	if(!_dirty) return; 	// we are clean
	size_t pos=0;
	for(size_t i=0; i<columns.size(); i++) {
		// loop invariants: 
		//  (1) pos <= i
		//  (2) the range [0:pos) contains nonnulls
		//  (3) every initial nonnull in [0:i) is in [0:pos)

		if(columns[i]) {
			if(pos < i) {
				assert(columns[pos]==nullptr); 
				columns[pos] = columns[i];  

				assert(columns[pos]->_index == i); 
				columns[pos]->_index = pos; 
			}
			pos++;
		}
	}
	assert(pos<columns.size()); // since we were dirty!
	columns.resize(pos);		// prune the vector
	_dirty = false; 			// we ar clean!
}

void output_table::add(basic_column& col) 
{
	_check_unlocked();

	_cleanup();

	if(col._table)
		throw std::runtime_error("column already added to a table");
	if(colnames.count(col.name())!=0) {
		throw std::runtime_error(binc::sprint("a column by this name already exists:",col.name()));
	}
	col._table = this;
	col._index = columns.size();
	columns.push_back(&col);
	colnames[col.name()] = &col;
}


void output_table::remove(basic_column& col)
{
	_check_unlocked();
	if(col._table != this) 
		throw std::invalid_argument(
			"output_table::remove(col) column not bound to this table");
	assert(columns[col._index]==&col);
	columns[col._index] = nullptr;
	colnames.erase(col.name());
	assert(colnames.find(col.name())==colnames.end());
	col._table = nullptr;
	_dirty = true;
}


void output_table::emit_row()
{
	if(files.empty())
		return;
	if(!_locked)
		throw std::logic_error("prolog() has not been called before emit_row()");
	// is the table enabled?
	if(!en) return;
	// ok, we are enabled
	for(auto b : bindings())
		if(b->enabled){
			// for every enabled binding
			b->file->output_row(*this);
		}
}

void output_table::prolog()
{
	// repack the table after possible column removals
	_cleanup();

	// do this for every bound file, enabled or not
	for(auto b : bindings())
		b->file->output_prolog(*this);

	// we are ready for business
	_locked = true;
}

void output_table::epilog()
{
	// changes are allowed
	_locked = false;

	// do this for every bound file, enabled or not
	for(auto b : bindings())
		b->file->output_epilog(*this);
}


result_table::result_table(const string& _name)
	: output_table(_name, table_flavor::RESULTS)
{
	//using std::cerr;
	//using std::endl;
	//cerr << "result table " << name() << " created"<< endl;
}

result_table::result_table(const string& _name,
		column_list col)
	: output_table(_name, table_flavor::RESULTS)
{
	add(col);

	//using std::cerr;
	//using std::endl;
	//cerr << "result table " << name() << " created"<< endl;
}

void result_table::add(column_list col)
{
	for(auto c : col) this->output_table::add(*c);	
}

result_table::~result_table()
{
	using std::cerr;
	using std::endl;
	//cerr << "result table " << name() << " destroyed"<< endl;
}


time_series::time_series(const string& _name) 
: output_table(_name, table_flavor::TIMESERIES), 
	now("time", "%d", std::bind(&context::now, &CTX)) 
{ add(now); }


output_file::output_file()
{ }

output_file::~output_file()
{
	output_binding::unbind_all(tables);
}

//-------------------------------------
//
// C (STDIO) files
//
//-------------------------------------

//-------------------------------------
//
// Formatters
//
//-------------------------------------


formatter::formatter(output_c_file* of, output_table& tab)
	: ofile(of), table(tab)
{ }

formatter::~formatter()
{ }


struct csvtab_formatter : formatter
{
	using formatter::formatter;
	void prolog() override;
	void row() override;
	void epilog() override;
};

void csvtab_formatter::prolog() 
{
	/*
		Logic for prolog:
		When the file is seekable, check if we are at the beginning
		before we output a header.
	 */
	long int fpos = ftell(ofile->file());
	if(fpos == -1 || fpos == 0) {
		for(size_t col=0;col < table.size(); col++) {
			if(col) fputs(",", ofile->file());
			fputs(table[col]->name().c_str(), ofile->file());
		}
		fputs("\n", ofile->file());
	} 
}

void csvtab_formatter::row() 
{
	for(size_t col=0;col < table.size(); col++) {
		if(col) fputs(",", ofile->file());
		table[col]->emit(ofile->file());
	}
	fputs("\n", ofile->file());	
}

void csvtab_formatter::epilog() 
{ }


struct csvrel_formatter : formatter
{
	using formatter::formatter;

	void prolog() override { }

	void row() override {
		fputs(table.name().c_str(), ofile->file());
		for(size_t col=0;col < table.size(); col++) {
			fputs(",", ofile->file());
			table[col]->emit(ofile->file());
		}
		fputs("\n", ofile->file());	
	}

	void epilog() override { }
};



formatter* formatter::create(output_c_file* f, output_table& t, text_format fmt)
{
	switch(fmt)
	{
	case text_format::csvtab:
		return new csvtab_formatter(f,t);
	case text_format::csvrel:
		return new csvrel_formatter(f,t);
	default:
		assert(0);
	}
}

void formatter::destroy(formatter* fmt)
{
	delete fmt;
}

//-------------------------------
// Methods
//-------------------------------

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

void output_c_file::open(FILE* _file, bool _owner)
{
	if(stream) 
		throw std::runtime_error("output file already open");
	stream = _file;
	owner = _owner;
}

void output_c_file::close()
{
	// handle the stream
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


output_c_file::output_c_file(FILE* _stream, bool _owner, text_format f)
: stream(_stream), owner(_owner), fmt(f) { }


output_c_file::output_c_file(const string& _fpath, open_mode mode, text_format f)
: output_c_file(f)
{
	open(_fpath, mode);
}


output_c_file::~output_c_file()
{
	close();
	// remove any open formatters
	for(auto&& f : fmtr)
		formatter::destroy(f.second);
}


void output_c_file::output_prolog(output_table& table)
{
	// Create formatter for table
	if(fmtr.count(&table)>0) return;
	auto form = fmtr[&table] = formatter::create(this, table, fmt);
	form->prolog();
}

void output_c_file::output_row(output_table& table)
{
	fmtr.at(&table)->row();
}

void output_c_file::output_epilog(output_table& table)
{ 
	auto form = fmtr.at(&table);
	form->epilog();

	fmtr.erase(&table);
	formatter::destroy(form);
}




output_c_file dds::output_stdout(stdout, false);
output_c_file dds::output_stderr(stderr, false);


//-------------------------------------
//
// Progress bar
//
//-------------------------------------


output_mem_file::output_mem_file(text_format fmt)
	: output_c_file(fmt), state(0)
{
	state = new memstate();
	state->buffer = nullptr;
	state->len = 0;
	FILE* f = open_memstream(&state->buffer, &state->len);
	open(f, true);
}

output_mem_file::~output_mem_file()
{
	if(stream != nullptr) 
		close();
	if(state) {
		free(state->buffer);
		delete state;
	}

}

const char* output_mem_file::contents()
{
	fflush(stream);
	return state->buffer;
}

string output_mem_file::str()
{
	return string(contents());
}



//-------------------------------------
//
// Progress bar
//
//-------------------------------------



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



/*
 *
 * This is the code for the table_handler
 *
 */



std::map<type_index, H5::DataType> __pred_type_map = 
{
	{typeid(bool), H5::PredType::NATIVE_UCHAR},

	{typeid(char), H5::PredType::NATIVE_CHAR},
	{typeid(signed char), H5::PredType::NATIVE_SCHAR},	
	{typeid(short), H5::PredType::NATIVE_SHORT},
	{typeid(int), H5::PredType::NATIVE_INT},
	{typeid(long), H5::PredType::NATIVE_LONG},
	{typeid(long long), H5::PredType::NATIVE_LLONG},

	{typeid(unsigned char), H5::PredType::NATIVE_UCHAR},
	{typeid(unsigned short), H5::PredType::NATIVE_USHORT},
	{typeid(unsigned int), H5::PredType::NATIVE_UINT},
	{typeid(unsigned long), H5::PredType::NATIVE_ULONG},
	{typeid(unsigned long long), H5::PredType::NATIVE_ULLONG},

	{typeid(float), H5::PredType::NATIVE_FLOAT},
	{typeid(double), H5::PredType::NATIVE_DOUBLE},
	{typeid(long double), H5::PredType::NATIVE_LDOUBLE}
};


H5::DataType hdf_mapped_type(basic_column* col)
{
	// make it simple-minded for now
	using namespace std::string_literals;
	auto predit = __pred_type_map.find(col->type());
	if(predit!=__pred_type_map.end()) 
		return predit->second;
	else if(col->type() == typeid(string)) {
		return H5::StrType(0, col->size());
	} else {
		throw std::logic_error("HDF5 mapping for type '"s+
			col->type().name()+"' not known"s);
	}
}


inline static size_t __aligned(size_t pos, size_t al) {
	assert( (al&(al-1)) == 0); // al is a power of 2
	return al*((pos+al-1)/al);
}

void output_hdf5::table_handler::make_row(char* buffer) 
{
	size_t pos = 0;
	for(size_t i=0; i< table.size(); i++) {
		if(i>0) pos = __aligned(pos+table[i-1]->size(), table[i]->align());
		assert(pos == colpos[i]);
		table[i]->copy(buffer + pos);
	}
}

output_hdf5::table_handler::table_handler(output_table& _table) 
: table(_table), colpos(table.size(),0), size(0), align(1)
{
	// first compute the size of the whole
	// thing
	for(size_t i=0;i<table.size();i++) {
		align = std::max(align, table[i]->align());
		if(i>0) size = __aligned(size, table[i]->align());
		size += table[i]->size();
	}
	if(table.size()>0) size = __aligned(size, table[0]->align());
	// now, compute the type
	size_t pos = 0;
	type = H5::CompType(size);
	for(size_t i=0;i<table.size();i++) {
		basic_column* c = table[i];
		if(i>0) pos = __aligned(pos+table[i-1]->size(), table[i]->align());
		colpos[i] = pos;
		type.insertMember(c->name(), pos, hdf_mapped_type(c));
	}
}

void output_hdf5::table_handler::create_dataset(const H5::Group& loc)
{
	using namespace H5;
	// it does not! create it
	hsize_t zdim[] = { 0 };
	hsize_t cdim[] = { 16 };
	hsize_t mdim[] = { H5S_UNLIMITED };
	DataSpace dspace(1, zdim, mdim);
	DSetCreatPropList props;
	props.setChunk(1, cdim);

	dataset = loc.createDataSet(table.name(), 
			type, dspace, props);		
}

void output_hdf5::table_handler::append_row()
{
	using namespace H5;
	// Make the image of an object.
	// This need not be aligned as far as I can tell!!!!!
	char buffer[size];
	memset(buffer,0,size); // this should silence valgrind
	make_row(buffer);

	/*
	Note: the following function is not yet supported in the 
	hdf5 version of Ubuntu :-(

	H5DOappend(dataset.getId(), H5P_DEFAULT, 0, 1, type, buffer);

	So, we must do the append "manually"
	*/

	// extend the dataset by one row
	DataSpace tabspc = dataset.getSpace();
	assert(tabspc.getSimpleExtentNdims()==1);
	hsize_t ext[1];
	tabspc.getSimpleExtentDims(ext);
	ext[0]++;
	dataset.extend(ext);

	// create table space
	tabspc = dataset.getSpace();
	hsize_t dext[] = { 1 };
	ext[0]--;  // use the old extent
	tabspc.selectHyperslab(H5S_SELECT_SET, dext, ext);
	DataSpace memspc;  // scalar dataspace

	dataset.write(buffer, type, memspc, tabspc);
}

output_hdf5::table_handler::~table_handler() 
{
	dataset.close();
}


/*
 *
 * This is the code for the main object  
 *
 */

output_hdf5::~output_hdf5()
{
	H5_CHECK(H5Idec_ref(locid));
}


output_hdf5::output_hdf5(long int _locid, open_mode _mode)
: locid(_locid), mode(_mode)
{
	H5_CHECK(H5Iinc_ref(locid));
}

output_hdf5::output_hdf5(const H5::Group& _group, open_mode _mode)
: output_hdf5(_group.getId(), _mode)
{  }


output_hdf5::output_hdf5(const H5::H5File& _file, open_mode _mode)
: output_hdf5(_file.openGroup("/"), _mode)
{  }


output_hdf5::output_hdf5(const string& h5file, open_mode mode)
: output_hdf5(H5::H5File(h5file, H5F_ACC_TRUNC), mode)
{ }


output_hdf5::table_handler* output_hdf5::handler(output_table& table)
{
	auto it = _handler.find(&table);
	if(it==_handler.end()) {
		table_handler* sc = new table_handler(table);
		_handler[&table] = sc;
		return sc;
	} else
		return it->second;
}



void output_hdf5::output_prolog(output_table& table)
{
	using namespace H5;

	// construct the table or timeseries
	Group loc(locid);
	table_handler* th = handler(table);
	
	// check the open mode and work accordingly
	if(this->mode == open_mode::append) {
		// check if an object by the given name exists in the loc
		if(hdf5_exists(locid, table.name())) {
			DataSet dset = loc.openDataSet(table.name());

			// ok, the dataset exists, just check compatibility
			hid_t dset_type = H5_CHECK(H5Dget_type(dset.getId()));

			if(! (th->type == DataType(dset_type)))
				throw std::runtime_error("On appending to HDF table,"\
					" types are not compatible");

			th->dataset = dset;

		} else {
			th->create_dataset(loc);
		}
	} else {
		assert(mode==open_mode::truncate);
		// maybe it exists, erase it
		if(hdf5_exists(locid, table.name())) {
			loc.unlink(table.name());
		}
		th->create_dataset(loc);
	}


}


void output_hdf5::output_row(output_table& table)
{	
	using namespace H5;
	table_handler* th = handler(table);
	th->append_row();

}


void output_hdf5::output_epilog(output_table& table)
{
	// just delete the handler
	auto it = _handler.find(&table);
	if(it != _handler.end()) {
		delete it->second;
		_handler.erase(it);		
	}
}


//-------------------------------------
//
// utilities
//
//-------------------------------------



enum_repr<text_format> dds::text_format_repr (
{
	{text_format::csvtab, "csvtab"},
	{text_format::csvrel, "csvrel"}
});

enum_repr<open_mode> dds::open_mode_repr ({
	{open_mode::truncate, "truncate"},
	{open_mode::append, "append"}
});

