#ifndef __OUTPUT_TESTS_HH__
#define __OUTPUT_TESTS_HH__

#include "output.hh"
#include "binc.hh"
#include <cxxtest/TestSuite.h>


#include <cstddef>
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


hdf5_dataset::~hdf5_dataset()
{
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


void hdf5_dataset::output_prolog(output_table& table)
{

}


void hdf5_dataset::output_row(output_table&)
{

}


void hdf5_dataset::output_epilog(output_table&)
{

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


	void test_hdf5()
	{
		using namespace H5;
		using namespace std::string_literals;

		H5File f("test.h5", H5F_ACC_TRUNC);
		hdf5_dataset ds(f,"run1");

		// make a compound datatype with an int, a double and
		// a string
		struct mytype {
			short int n;
			double x;
			char foo[20];
		} data[4] = {
			{1, 2.1, "line1"},
			{2, 2.2, "line2"},
			{3, 2.3, "line3"},
			{4, 2.4, "line4"}
		};

		CompType h5_mytype(sizeof(mytype));
		h5_mytype.insertMember("n", offsetof(mytype,n), PredType::STD_I16LE);
		h5_mytype.insertMember("x", offsetof(mytype,x), PredType::NATIVE_DOUBLE);
		h5_mytype.insertMember("foo_3", offsetof(mytype,foo), StrType(PredType::C_S1,19));

		hsize_t dim[] = { 4 };
		DataSpace dspace(1,dim);
		
		DataSet dset = ds.loc.createDataSet("testdata", h5_mytype, dspace);
		dset.write(data, h5_mytype);

		{
			FL_PacketTable pckt(ds.loc.getId(), "packet_table", 
				h5_mytype.getId(), 16384);
			TS_ASSERT( pckt.IsValid() );
		}

		//pckt.close();
		FL_PacketTable pt(ds.loc.getId(), "packet_table");
		pt.AppendPackets(2, data);

		mytype indata[4];

		dset.read(indata, h5_mytype);
		for(size_t i=0;i<4;i++) {
			TS_ASSERT_EQUALS(data[i].n, indata[i].n);
			TS_ASSERT_EQUALS(data[i].x, indata[i].x);
			TS_ASSERT_EQUALS(data[i].foo, indata[i].foo);
		}


	}



};


#endif
