#ifndef __OUTPUT_HH__
#define __OUTPUT_HH__

#include <string>
#include <vector>
#include <cstdio>

#include "dds.hh"

namespace dds
{

using std::string;


/*-----------------------------
	Generic classes
  -----------------------------*/

class basic_column : public named
{
protected:
	string _format;
public:
	basic_column(const char* _name, const char* f)
	: named(_name), _format(f) { }
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
public:
	output_table(const char* _name)
	: named(_name), en(true) 
	{ }

	output_table(const char* _name, std::initializer_list<basic_column *> col)
	: named(_name), en(true), columns(col)
	{ }

	inline void add(basic_column& col) { columns.push_back(&col); }
	
	inline void set_enabled(bool _en) { en=_en; }
	inline bool enabled() const { return en; }

	virtual void emit_header_start(FILE*);
	virtual void emit_row_start(FILE*);

	void emit_header(FILE*);
	void emit(FILE*);
};


class time_series : public output_table
{
public:
	time_series(const char* _name) 
	: output_table(_name), now("time", "%d") {}

	column<dds::timestamp> now;

	void emit_header_start(FILE*) override;
	void emit_row_start(FILE*) override;
};



} // end namespace dds


#endif