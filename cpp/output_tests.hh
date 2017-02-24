#ifndef __OUTPUT_TESTS_HH__
#define __OUTPUT_TESTS_HH__

#include "output.hh"
#include "binc.hh"
#include <cxxtest/TestSuite.h>


#include <cstddef>
#include <functional>

#include <H5PacketTable.h>

using binc::print;

namespace dds {

/**
	Output file to an HDF5 dataset.

  */

class hdf5_dataset : public output_file
{
public:
	H5::Group loc;
	open_mode mode;

	struct table_handler;
	std::map<output_table*, table_handler*> _handler;
	table_handler* handler(output_table&);

	/**
		Use the file or group for the dataset.
	  */
	hdf5_dataset(const H5::H5File& _fg, open_mode mode=default_open_mode);
	hdf5_dataset(const H5::Group& _fg, open_mode mode=default_open_mode);

	/**
		Create a new group in base, by the given name and place the
		dataset there.
	  */
	hdf5_dataset(const H5::CommonFG& base, const string& name, 
		open_mode m=default_open_mode);

	virtual void output_prolog(output_table&);
	virtual void output_row(output_table&);
	virtual void output_epilog(output_table&);

	~hdf5_dataset();
};

} // end namespace dds


using namespace dds;


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


struct hdf5_dataset::table_handler
{
	output_table& table;
	std::vector<size_t> colpos;
	size_t size;
	size_t align;
	H5::CompType type;
	H5::DataSet dataset;

	inline static size_t __aligned(size_t pos, size_t al) {
		assert( (al&(al-1)) == 0); // al is a power of 2
		return al*((pos+al-1)/al);
	}

	void make_row(char* buffer) {
		size_t pos=0;
		for(size_t i=0; i<table.size();i++) {
			if(i>0) {
				// advance pos to subsume the size of 
				// column i-1 and the alignment of column i
				size_t al = table[i]->align();
				pos = __aligned(pos,al);
			}
			table[i]->copy(buffer+pos);
			pos += table[i]->size();
		}
		assert(size == __aligned(pos, table[0]->align()));
	}

	table_handler(output_table& _table) 
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

	void create_dataset(const H5::Group& loc)
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

	void append_row()
	{
		using namespace H5;
		// Make the image of an object.
		// This need not be aligned as far as I can tell!!!!!
		char buffer[size];
		size_t pos = 0;
		for(size_t i=0; i< table.size(); i++) {
			if(i>0) pos = __aligned(pos+table[i-1]->size(), table[i]->align());
			assert(pos == colpos[i]);
			table[i]->copy(buffer + pos);
		}

		// extend the dataset by one row

		DataSpace tabspc = dataset.getSpace();
		assert(tabspc.getSimpleExtentNdims()==1);
		hsize_t oldext[1];
		tabspc.getSimpleExtentDims(oldext);
		hsize_t newext[] = { oldext[0]+1 };
		dataset.extend(newext);

		// create table space
		tabspc = dataset.getSpace();
		hsize_t dext[] = { 1 };
		tabspc.selectHyperslab(H5S_SELECT_SET, dext, oldext);
		DataSpace memspc(1, dext);

		dataset.write(buffer, type, memspc, tabspc);
	}
};


hdf5_dataset::~hdf5_dataset()
{
	loc.close();
}

hdf5_dataset::hdf5_dataset(const H5::H5File& _file, open_mode _mode)
: loc(_file.openGroup("/")), mode(_mode)
{ }

hdf5_dataset::hdf5_dataset(const H5::Group& _group, open_mode _mode)
: loc(_group), mode(_mode)
{ }


hdf5_dataset::hdf5_dataset (const H5::CommonFG& base, 
										const string& name, open_mode _mode)
: loc(base.createGroup(name)), mode(_mode)
{ }


hdf5_dataset::table_handler* hdf5_dataset::handler(output_table& table)
{
	auto it = _handler.find(&table);
	if(it==_handler.end()) {
		table_handler* sc = new table_handler(table);
		_handler[&table] = sc;
		return sc;
	} else
		return it->second;
}


void hdf5_dataset::output_prolog(output_table& table)
{
	using namespace H5;

	// construct the table or timeseries
	table_handler* th = handler(table);
	
	// check the open mode and work accordingly
	if(mode == open_mode::append) {
		// check if an object by the given name exists in the loc
		print("APPENDING");
		try {

			DataSet dset = loc.openDataSet(table.name());
			// ok, it exists, just check compatibility
			if(! (th->type == DataType(H5Dget_type(dset.getId()))))
				throw std::runtime_error("On appending to HDF table,"\
					" types are not compatible");

			th->dataset = dset;
		} catch(Exception& e) {
			print("CREATING FROM SCRATCH");
			th->create_dataset(loc);
		}
	} else {
		print("TRUNCATING");
		assert(mode==open_mode::truncate);
		// maybe it exists, erase it
		// TODO
		th->create_dataset(loc);
	}


}


void hdf5_dataset::output_row(output_table& table)
{	
	using namespace H5;
	table_handler* th = handler(table);
	th->append_row();

}


void hdf5_dataset::output_epilog(output_table& table)
{
	// just delete the handler
	auto it = _handler.find(&table);
	if(it != _handler.end()) {
		delete it->second;
		_handler.erase(it);		
	}
}




class OutputTestSuite : public CxxTest::TestSuite
{
public:

	struct _silly {
		column<int> count {"count", "%d"};
		column<double> mean_x {"mean_x", "%f" };
		column<std::string> label {"label", 12, "%s" };
	};

	struct silly_table : _silly, result_table
	{
		silly_table(const char* n) 
		: result_table(n, {&count, &mean_x, &label}) {}
	};

	void test_column() 
	{
		column<string> s{"s",10,"%s"};
		s = "Hello cruel world";
		TS_ASSERT_EQUALS(s.value(), string("Hello crue"));
		TS_ASSERT_EQUALS(s.size(), sizeof(char)*11);
		TS_ASSERT_EQUALS(s.align(), alignof(char));
	}

	void test_output()
	{
		silly_table tab("SILLY");

		tab.count = 1;
		tab.mean_x = 3.14;
		tab.label = "Hello";

		char* buf = NULL;
		size_t len = 0;
		FILE* mf = open_memstream(&buf, &len);
		output_file* f = new output_c_file(mf);
		f->bind(tab);

		tab.prolog();
		tab.emit_row();
		tab.emit_row();
		tab.emit_row();
		tab.epilog();

		delete f;
		fclose(mf);

		const char* expected =
		"count,mean_x,label\n"
		"1,3.140000,Hello\n"
		"1,3.140000,Hello\n"
		"1,3.140000,Hello\n";

		TS_ASSERT_EQUALS( buf, expected );
		free(buf);
	}

	void test_time_series()
	{
		time_series T("series");

		column<double> t1 { "t1", "%f" };
		column<double> t2 { "t2", "%f" };

		T.add(t1);
		T.add(t2);

		//T.emit_header(stdout);

		t1 = 13.2;
		t2 = 11.4;
		for(dds::timestamp t=10; t < 12; t++) {
			T.now = t;
			t1 = t1.value() + t;
			t2 = t2.value() - t;
			//T.emit(stdout);
		}
	}


	void test_output_file() 
	{
		output_c_file* fset[2];

		fset[0] = new output_c_file(fmemopen(NULL, 8192, "w+"),true);
		fset[1] = new output_c_file(fmemopen(NULL, 8192, "w+"),true);

		silly_table tab("SILLY");

		tab.count = 1;
		tab.mean_x = 3.14;
		tab.label = "Hello";

		tab.bind_all(fset);
		tab.prolog();
		tab.emit_row();

		char buf[2][8192];
		fseek(fset[0]->file(), 0, SEEK_SET);
		fseek(fset[1]->file(), 0, SEEK_SET);
		TS_ASSERT_EQUALS(fread(buf[0], 1, 8191, fset[0]->file()), 36);
		TS_ASSERT_EQUALS(fread(buf[1], 1, 8191, fset[1]->file()), 36);
		buf[0][50] = buf[1][50] = 0;
		TS_ASSERT_EQUALS( buf[0], buf[1] );
		auto expected = "count,mean_x,label\n"
						"1,3.140000,Hello\n";
		TS_ASSERT_EQUALS( buf[0], expected );

		delete fset[0];
		delete fset[1];
	}

	void test_bind() 
	{
		silly_table T1("table1"), T2("table2");

		output_file* f1 = new output_c_file("/dev/null");
		output_file* f2 = new output_c_file("/dev/null");
		output_file* f3 = new output_c_file("/dev/null");

		T1.bind(f1);
		TS_ASSERT_EQUALS(T1.bindings().size(), 1);
		TS_ASSERT_EQUALS(T2.bindings().size(), 0);
		TS_ASSERT_EQUALS(f1->bindings().size(), 1);

		T1.bind(f1);
		TS_ASSERT_EQUALS(T1.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 1);

		f1->bind(T1);
		TS_ASSERT_EQUALS(T1.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 1);

		f2->bind(T1);
		T1.bind(f3);
		f1->bind(T2);
		TS_ASSERT_EQUALS(T1.bindings().size(), 3);
		TS_ASSERT_EQUALS(T2.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 2);
		TS_ASSERT_EQUALS(f2->bindings().size(), 1);
		TS_ASSERT_EQUALS(f3->bindings().size(), 1);

		f2->unbind(T2);
		TS_ASSERT_EQUALS(T1.bindings().size(), 3);
		TS_ASSERT_EQUALS(T2.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 2);
		TS_ASSERT_EQUALS(f2->bindings().size(), 1);
		TS_ASSERT_EQUALS(f3->bindings().size(), 1);

		delete f3;
		TS_ASSERT_EQUALS(T1.bindings().size(), 2);
		TS_ASSERT_EQUALS(T2.bindings().size(), 1);
		TS_ASSERT_EQUALS(f1->bindings().size(), 2);
		TS_ASSERT_EQUALS(f2->bindings().size(), 1);

		delete f1;
		delete f2;
		TS_ASSERT_EQUALS(T1.bindings().size(), 0);
		TS_ASSERT_EQUALS(T2.bindings().size(), 0);
	}


	struct dummy_table : result_table
	{
		column<bool> bool_attr { "bool_attr", "%d"};
		column<stream_id> sid { "sid", "%hd" };
		column<source_id> hid { "hid", "%hd" };
		column<double> zeta { "zeta", "%.10g" };
		column<size_t> nsize { "nsize", "%zu" };
		column<string> mname { "mname", 31, "%s" };

		dummy_table(const string& name) : result_table(name)
		{
			add({
				& bool_attr,
				& sid,
				& hid,
				& zeta,
				& nsize,
				& mname
			});
		}

		void fill_columns(size_t i)
		{
			using binc::sprint;
			bool_attr = (i%3)==1;
			sid = i;
			hid = i;
			zeta = i/2.;
			nsize = i*2;
			mname = sprint("this is record",i);
		}
	};

	/* Expected layout: same as c struct */
	struct __dummy_rec {
		bool 		bool_attr;
		stream_id  	sid;
		source_id	hid;
		double 		zeta;
		size_t 		nsize;
		char 		mname[32]; // note: maxlen+1
	};


	void test_hdf5_dataset_table_handler_schema()
	{
		using namespace H5;

		dummy_table dummy("dummy_table");
		hdf5_dataset::table_handler* handler = 
			new hdf5_dataset::table_handler(dummy);

		TS_ASSERT_EQUALS(& handler->table, &dummy);

		TS_ASSERT_EQUALS(handler->size, sizeof(__dummy_rec));
		TS_ASSERT_EQUALS(handler->align, alignof(__dummy_rec));
		TS_ASSERT_EQUALS(handler->colpos[0], offsetof(__dummy_rec,bool_attr));
		TS_ASSERT_EQUALS(handler->colpos[1], offsetof(__dummy_rec,sid));
		TS_ASSERT_EQUALS(handler->colpos[2], offsetof(__dummy_rec,hid));
		TS_ASSERT_EQUALS(handler->colpos[3], offsetof(__dummy_rec,zeta));
		TS_ASSERT_EQUALS(handler->colpos[4], offsetof(__dummy_rec,nsize));
		TS_ASSERT_EQUALS(handler->colpos[5], offsetof(__dummy_rec,mname));
	}


	void check_dummy_dataset(H5::DataSet dataset, size_t Nrec)
	{
		using namespace H5;
		// this is just to use fill_columns
		dummy_table dummy("dummy_table");

		DataSpace dspc = dataset.getSpace();
		TS_ASSERT_EQUALS(dspc.getSimpleExtentNdims(), 1);
		hsize_t dims[1];
		dspc.getSimpleExtentDims(dims);
		TS_ASSERT_EQUALS((size_t) dims[0], Nrec);

		__dummy_rec data[Nrec];
		//handler->dataset.read(data, handler->type, dspc, dspc);
		CompType type(sizeof(__dummy_rec));
		type.insertMember("bool_attr", offsetof(__dummy_rec,bool_attr), 
			PredType::NATIVE_UCHAR);
		type.insertMember("sid", offsetof(__dummy_rec,sid), 
			PredType::NATIVE_SHORT);
		type.insertMember("hid", offsetof(__dummy_rec,hid), 
			PredType::NATIVE_SHORT);
		type.insertMember("zeta", offsetof(__dummy_rec,zeta), 
			PredType::NATIVE_DOUBLE);
		type.insertMember("nsize", offsetof(__dummy_rec,nsize),
			PredType::NATIVE_HSIZE);
		type.insertMember("mname", offsetof(__dummy_rec,mname), 
			StrType(0,32));

		dataset.read(data, type);
		for(size_t i=0; i<Nrec; i++) {
			dummy.fill_columns(i);
			TS_ASSERT_EQUALS(data[i].bool_attr, dummy.bool_attr.value());
			TS_ASSERT_EQUALS(data[i].sid, dummy.sid.value());
			TS_ASSERT_EQUALS(data[i].hid, dummy.hid.value());
			TS_ASSERT_EQUALS(data[i].zeta, dummy.zeta.value());
			TS_ASSERT_EQUALS(data[i].nsize, dummy.nsize.value());
			TS_ASSERT_EQUALS(string(data[i].mname), dummy.mname.value());
		}

	}

	void test_hdf5_dataset_table_handler_data()
	{
		using namespace H5;

		dummy_table dummy("dummy_table");
		hdf5_dataset::table_handler* handler = 
			new hdf5_dataset::table_handler(dummy);
	
		auto file = H5File("dummy_file.h5", H5F_ACC_TRUNC);
		Group loc = file.openGroup("/");
		handler->create_dataset(loc);

		using binc::sprint;
		size_t Nrec = 100;
		for(size_t i=0; i<Nrec; i++) {
			dummy.fill_columns(i);
			handler->append_row();
		}

		check_dummy_dataset(handler->dataset, Nrec);		
		delete handler;
	}


	void test_hdf5_dataset_basic()
	{
		using namespace std::string_literals;
		using namespace H5;
		using std::reference_wrapper;
		using std::ref;

		dummy_table dummy1("dummy1");
		dummy_table dummy2("dummy2");
		dummy_table dummy3("dummy3");

		std::vector<reference_wrapper<dummy_table>> tables {
			dummy1, dummy2, dummy3 
		};

		auto file = H5File("dummy_file2.h5", H5F_ACC_TRUNC);
		hdf5_dataset dset(file);

		for(output_table& t : tables) {
			dset.bind(t);
			t.prolog();
		}

		for(size_t i=0; i<10; i++) {
			for(dummy_table& t : tables) {
				t.fill_columns(i);
				t.emit_row();
			}	
		}

		for(output_table& t : tables) {
			t.epilog();
		}

		check_dummy_dataset(file.openDataSet("dummy1"), 10);
		check_dummy_dataset(file.openDataSet("dummy2"), 10);
		check_dummy_dataset(file.openDataSet("dummy3"), 10);		
	}

	void test_hdf5_dataset_truncate()
	{
		using namespace std::string_literals;
		using namespace H5;

		dummy_table dummy("dummy");
		auto file = H5File("dummy_file3.h5", H5F_ACC_TRUNC);
		auto dset= new hdf5_dataset(file, open_mode::truncate);

		dset->bind(dummy);
		dummy.prolog();
		for(size_t i=0; i<10; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		check_dummy_dataset(file.openDataSet("dummy"), 10);

		dset = new hdf5_dataset(file, open_mode::truncate);
		dummy.prolog();
		for(size_t i=0; i<5; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		check_dummy_dataset(file.openDataSet("dummy"), 5);
	}

	void test_hdf5_dataset_append()
	{
		using namespace std::string_literals;
		using namespace H5;

		dummy_table dummy("dummy");
		auto file = H5File("dummy_file4.h5", H5F_ACC_TRUNC);
		auto dset= new hdf5_dataset(file, open_mode::truncate);

		dset->bind(dummy);
		dummy.prolog();
		for(size_t i=0; i<10; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		check_dummy_dataset(file.openDataSet("dummy"), 10);

		dset = new hdf5_dataset(file, open_mode::append);
		dummy.prolog();
		for(size_t i=10; i<25; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		//check_dummy_dataset(file.openDataSet("dummy"), 25);
	}


};


#endif
