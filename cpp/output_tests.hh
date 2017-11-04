#ifndef __OUTPUT_TESTS_HH__
#define __OUTPUT_TESTS_HH__

#include <functional>

#include "output.hh"
#include "binc.hh"

#include <cxxtest/TestSuite.h>
#include "hdf5_util.hh"


using binc::print;


using namespace dds;


struct table_mixin1
{
	column<size_t>  foo  { "foo", "%zu" };
	column<string>  bar  { "bar", 32, "%s" };
	table_mixin1(result_table* host) {
		host->add({& foo, &bar});
	}
};

struct table_mixin2 
{
	column<size_t>  bla  { "bla", "%zu" };
	column<string>  baz  { "baz", 32, "%s" };
	table_mixin2(result_table* host) {
		host->add({& bla, &baz});		
	}
};


struct myresults  : result_table, table_mixin1, table_mixin2 
{
	column<string> mystring  { "mystring", 20, "%s" };

	myresults() : result_table("myresults"), 
		table_mixin1(this), table_mixin2(this) 
	{
		add({ &mystring });
	}
};

namespace dds { class OutputTestSuite; }

class dds::OutputTestSuite : public CxxTest::TestSuite
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
		output_file* f = new output_c_file(mf, false, text_format::csvtab);
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



	void test_output_file() 
	{
		output_c_file* fset[2];

		fset[0] = new output_c_file(fmemopen(NULL, 8192, "w+"),true, text_format::csvtab);
		fset[1] = new output_c_file(fmemopen(NULL, 8192, "w+"),true, text_format::csvtab);

		silly_table tab("SILLY");

		tab.count = 1;
		tab.mean_x = 3.14;
		tab.label = "Hello";

		tab.bind(fset[0]);
		tab.bind(fset[1]);
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


	void test_output_hdf5_table_handler_schema()
	{
		using namespace H5;

		dummy_table dummy("dummy_table");
		output_hdf5::table_handler* handler = 
			new output_hdf5::table_handler(dummy);

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
		dummy_table dummy("dummy_table2");

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

	void test_output_hdf5_table_handler_data()
	{
		using namespace H5;

		dummy_table dummy("dummy_table");
		output_hdf5::table_handler* handler = 
			new output_hdf5::table_handler(dummy);
	
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


	void test_output_hdf5_basic()
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
		output_hdf5 dset(file);

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

	void test_output_hdf5_truncate()
	{
		using namespace std::string_literals;
		using namespace H5;

		dummy_table dummy("dummy");
		auto file = H5File("dummy_file3.h5", H5F_ACC_TRUNC);
		auto dset= new output_hdf5(file, open_mode::truncate);

		dset->bind(dummy);
		dummy.prolog();
		for(size_t i=0; i<10; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		check_dummy_dataset(file.openDataSet("dummy"), 10);

		dset = new output_hdf5(file, open_mode::truncate);
		dset->bind(dummy);
		dummy.prolog();
		for(size_t i=0; i<5; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		check_dummy_dataset(file.openDataSet("dummy"), 5);
	}

	void test_output_hdf5_append()
	{
		using namespace std::string_literals;
		using namespace H5;

		dummy_table dummy("dummy");
		auto file = H5File("dummy_file4.h5", H5F_ACC_TRUNC);

		auto dset= new output_hdf5(file, open_mode::append);
		dset->bind(dummy);
		dummy.prolog();
		for(size_t i=0; i<10; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		check_dummy_dataset(file.openDataSet("dummy"), 10);

		dset = new output_hdf5(file, open_mode::append);

		TS_ASSERT_EQUALS(dset->mode, open_mode::append);
		dset->bind(dummy);
		dummy.prolog();
		for(size_t i=10; i<25; i++) {
			dummy.fill_columns(i);
			dummy.emit_row();
		}
		dummy.epilog();
		delete dset;

		check_dummy_dataset(file.openDataSet("dummy"), 25);
	}

	void test_settable()
	{
		column<double> double_foo("double", "%.10g", 0.0);
		column<bool> bool_foo("bool", "%c", 0);
		column<unsigned short> ushort_foo("ushort", "%hu", 0);
		column<long> long_foo("long", "%ld", 0);
		column<string> str_foo("str", 40, "%ld", "");

		TS_ASSERT_THROWS(
			dynamic_cast<basic_column&>(double_foo).set(string("Hello")), 
			std::invalid_argument);
		TS_ASSERT_THROWS( 
			dynamic_cast<basic_column&>(str_foo).set(3.14),  
			std::invalid_argument);

		bool_foo.set(0.2);
		TS_ASSERT_EQUALS(bool_foo.value(), true);

		ushort_foo.set(137.);
		TS_ASSERT_EQUALS(ushort_foo.value(), 137);

		long_foo.set(132437.);
		TS_ASSERT_EQUALS(long_foo.value(), 132437);

		double_foo.set(true);
		TS_ASSERT_EQUALS(double_foo.value(), 1.0);

		double_foo.set('A');
		TS_ASSERT_EQUALS(double_foo.value(), 65.0);

		double_foo.set(-165353);
		TS_ASSERT_EQUALS(double_foo.value(), -165353.);
	}


	void test_untyped()
	{
		column<double> double_foo("double", "%.10g", 0.0);
		column<bool> bool_foo("bool", "%c", 0);
		column<unsigned short> ushort_foo("ushort", "%hu", 0);
		column<long> long_foo("long", "%ld", 0);
		column<string> str_foo("str", 40, "%ld", "");

		result_table t("table");
		t.add({&double_foo, &bool_foo, &ushort_foo, &long_foo, &str_foo});

		TS_ASSERT( t["double"]->is_arithmetic() );
		TS_ASSERT( t["bool"]->is_arithmetic() );
		TS_ASSERT( t["ushort"]->is_arithmetic() );
		TS_ASSERT( t["long"]->is_arithmetic() );
		TS_ASSERT( !t["str"]->is_arithmetic() );

		TS_ASSERT_THROWS( t["none"], std::out_of_range);

		TS_ASSERT_THROWS(t["double"]->set(string("Hello")), 
			std::invalid_argument);
		TS_ASSERT_THROWS(t["str"]->set(3.14),
			std::invalid_argument);

		t["bool"]->set(0.2);
		TS_ASSERT_EQUALS(bool_foo.value(), true);

		t["ushort"]->set(137.);
		TS_ASSERT_EQUALS(ushort_foo.value(), 137);

		t["long"]->set(132437.);
		TS_ASSERT_EQUALS(long_foo.value(), 132437);

		t["double"]->set(true);
		TS_ASSERT_EQUALS(double_foo.value(), 1.0);

		t["double"]->set('A');
		TS_ASSERT_EQUALS(double_foo.value(), 65.0);

		t["double"]->set(-165353);
		TS_ASSERT_EQUALS(double_foo.value(), -165353.);
	}

	void fill_mixin1(table_mixin1& tm1)
	{
		tm1.foo = 10;
		tm1.bar = "Ho ho ho";
	}


	void test_myresults()
	{
		myresults table;
		using namespace std::string_literals;

		TS_ASSERT_EQUALS(table.flavor(), table_flavor::RESULTS);
		TS_ASSERT_EQUALS(table.foo.table(), &table);

		TS_ASSERT_EQUALS(table.size(), 5);

		output_mem_file f(text_format::csvtab);
		table.bind(&f);

		table.prolog();
		table.foo =1;
		table.bar = "Hello"s;
		table.bla = 2;
		table.baz = "world"s; 
		table.mystring = "My string";
		table.emit_row();
		fill_mixin1(table);
		table.emit_row();
		table.epilog();

		TS_ASSERT_EQUALS(f.str(),
			"foo,bar,bla,baz,mystring\n"
			"1,Hello,2,world,My string\n"
			"10,Ho ho ho,2,world,My string\n"
			);
	}


};


#endif
