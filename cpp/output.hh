#ifndef __OUTPUT_HH__
#define __OUTPUT_HH__

#include <string>
#include <vector>
#include <cstdio>
#include <list>
#include <algorithm>

#include "dds.hh"

namespace dds
{

using std::string;


/*-----------------------------
	Generic classes
  -----------------------------*/

class output_table;

class basic_column : public named
{
protected:
	output_table* _table;
	size_t _index;
	string _format;
	friend class output_table;
public:
	basic_column(const char* _name, const char* f)
	: named(_name), _table(0), _format(f) { }

	basic_column(const basic_column&)=delete;
	basic_column(const basic_column&&)=delete;
	virtual ~basic_column();

	inline const char* format() const { return _format.c_str(); }
	virtual void emit(FILE*) = 0;
};


template <typename T>
class column : public basic_column
{
protected:
	T val;
public:
	column(const char* _n, const char* fmt) 
	: basic_column(_n, fmt) { }
	inline T value() const { return val; }
	inline T& operator=(T v) { val=v; return val; }
	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}
};

template <>
class column<string> : public basic_column
{
protected:
	string val;
public:
	column(const char* _n, const char* fmt) 
	: basic_column(_n, fmt) { }

	inline const char* value() const { return this->val.c_str(); }
	inline string& operator=(const string& v) { val=v; return val; }
	void emit(FILE* s) override {
		fprintf(s, format(), value());
	}
};


class output_table : public named
{
protected:
	bool en;
	std::vector<basic_column *> columns;
	friend class basic_column;
	inline void __associate(basic_column* col, size_t _i) {
		if(col->_table)
			throw std::runtime_error("column already added to a table");
		col->_table = this;
		col->_index = _i;
	}
public:
	output_table(const char* _name)
	: named(_name), en(true) 
	{ }

	output_table(const char* _name, std::initializer_list<basic_column *> col);

	virtual ~output_table();

	inline void add(basic_column& col) {
		__associate(&col, columns.size());
		columns.push_back(&col); 
	}
	
	inline size_t size() const { return columns.size(); }
	inline basic_column* operator[](size_t i) const { return columns.at(i); }

	inline void set_enabled(bool _en) { en=_en; }
	inline bool enabled() const { return en; }

	virtual void emit_header_start(FILE*);
	virtual void emit_row_start(FILE*);

	void emit_header(FILE*);
	void emit(FILE*);
};


class output_file;
class result_table;
struct __binding;

struct __binding 
{
	typedef std::list<__binding*> list;
	typedef typename list::iterator iter;

	output_file* file;
	result_table* table;
	iter in_file_list, in_table_list;

	bool enabled;
	__binding(output_file* f, result_table* t);
	~__binding();
	static void unbind_all(list&);
	static __binding* find(list&, output_file*);
	static __binding* find(list&, result_table*);
};


class result_table : public output_table
{
protected:
	__binding::list files;
	friend struct __binding;
public:
	using output_table::output_table;

	virtual ~result_table();

	void bind(output_file* f) { 
		if(__binding::find(files, f)==0)
			new __binding(f, this); 
	}

	template <typename T>
	void bind_all(T& ctr) {
		std::for_each(std::begin(ctr), std::end(ctr), 
			[&](output_file* f){ bind(f); } );
	}

	void unbind(output_file* f) {
		auto b = __binding::find(files, f);
		if(b) delete b;
	}

	inline auto bindings() const { return files; }

	void unbind_all() {
		__binding::unbind_all(files);
	}

	void emit_header_row();
	void emit_row();
};


class time_series : public result_table
{
public:
	time_series(const char* _name) 
	: result_table(_name), now("time", "%d") {}

	time_series() : time_series("timeseries") {}

	column<dds::timestamp> now;

	void emit_header_start(FILE*) override;
	void emit_row_start(FILE*) override;

	void emit_row();
};


enum open_mode { truncate, append };

class output_file 
{
protected:
	FILE* stream;
	string filepath;
	bool owner;

	__binding::list tables;
	friend struct __binding;
public:

	output_file() : stream(0), filepath(), owner(false) {}
	output_file(FILE* _stream, bool _owner=false);
	output_file(const string& _fpath, open_mode mode = append);

	output_file(const output_file&)=delete;
	output_file& operator=(const output_file&)=delete;

	inline output_file(output_file&& other) 
	: stream(other.stream), filepath(other.filepath), owner(other.owner)
	{ other.stream = nullptr; }

	inline output_file& operator=(output_file&& other)
	{
		stream = other.stream;
		filepath = other.filepath;
		owner = other.owner;
		other.stream = nullptr;
		return *this;
	}

	virtual ~output_file();

	void bind(result_table& t) { 
		if(__binding::find(tables, &t)==0)
			new __binding(this, &t); 
	}
	void unbind(result_table& t) {
		auto b = __binding::find(tables, &t);
		if(b) delete b;
	}
	inline auto bindings() const { return tables; }
	void unbind_all() {
		__binding::unbind_all(tables);
	}

	virtual void open(const string& _fpath, open_mode mode = append);
	virtual void close();
	virtual void flush();

	inline FILE* file() const { return stream; }
	void set_owner(bool b) { owner = b; }
	bool is_owner() const { return owner; }

	const string& path() const { return filepath; }
};

/**
 * \brief Progress bar.
 *
 * This is a utility class printing a progress bar on the terminal.
 * At construction, a total number of ticks, N, is specified.
 * Then, as method tick() is called the progress bar is displayed.
 *
 */
class progress_bar
{
	typedef unsigned long long llint;
	FILE* stream;
	string message;
	size_t N,i,ni;
	llint B,l;
	bool finished;


	inline llint nexti() const { return (N*(l+1)+B-1)/B; }

	void adjustBar();

public:
	/**
	 * Construct the object.
	 * @param s logger to use for output.
	 * @param b an optional length for the progress bar, in characters.
	 * @param msg an optional message to print before the bar.
	 */
	progress_bar(FILE* , size_t b=40, const string& msg="");

	/**
	 * Start printing the bar and expect the given number of ticks.
	 * @param n total number of ticks expected
	 */
	void start(unsigned long long n);

	/**
	 * This method signals ticks to the progress bar.
	 */
	inline void tick(size_t ticks = 1) {
		if(finished) return;
		if( (i += ticks)>=ni )
        	adjustBar();
	}

    /**
     * If the parameter is greater than the current number of ticks,
     * set the ticks to the given value.
     */
    inline void complete(size_t ticks) {
            if(finished) return;
            if((ticks > ni) && (ticks>i)) tick(ticks-i);
    }

    /**
     * Finish the bar now, possibly early.
     */
    void finish();
};





} // end namespace dds


#endif