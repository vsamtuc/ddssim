#ifndef __HDF5_UTIL_HH__
#define __HDF5_UTIL_HH__

#include <string>
#include <H5Cpp.h>
#include "output.hh"

using std::string;
using namespace dds;


inline bool hdf5_exists(hid_t locid, const string& name)
{
	return H5Lexists(locid, name.c_str(), H5P_DEFAULT);
}


struct output_hdf5::table_handler
{
	output_table& table;
	std::vector<size_t> colpos;
	size_t size;
	size_t align;
	H5::CompType type;
	H5::DataSet dataset;


	table_handler(output_table& _table);
	void make_row(char* buffer);
	void create_dataset(const H5::Group& loc);
	void append_row();
	~table_handler();
};



#endif