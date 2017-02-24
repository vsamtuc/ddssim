#ifndef __OUTPUT_TESTS_HH__
#define __OUTPUT_TESTS_HH__

#include <functional>

#include "output.hh"
#include "binc.hh"

#include <cxxtest/TestSuite.h>
#include "hdf5_util.hh"


using binc::print;


using namespace dds;



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


};


#endif
