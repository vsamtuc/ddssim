
#include "dds.hh"
#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"

#include <boost/python.hpp>

namespace { // Avoid cluttering the global namespace.

  // Converts a std::pair instance to a Python tuple.
  template <typename T1, typename T2>
  struct std_pair_to_tuple
  {
    static PyObject* convert(std::pair<T1, T2> const& p)
    {
      return boost::python::incref(
        boost::python::make_tuple(p.first, p.second).ptr());
    }
    static PyTypeObject const *get_pytype () {return &PyTuple_Type; }
  };

	// Helper for convenience.
	template <typename T1, typename T2>
	struct std_pair_to_python_converter
	{
		std_pair_to_python_converter()
		{
			boost::python::to_python_converter<
				std::pair<T1, T2>,
				std_pair_to_tuple<T1, T2>,
				true //std_pair_to_tuple has get_pytype
			>();
		}
	};

	inline void __wrap_dataset_load(dds::dataset& D, 
		std::auto_ptr<dds::data_source> p)
	{
		D.load(p.get());
		p.release();
	}

	using namespace boost::python;

	struct output_pyfile : dds::output_c_file
	{
		object pyfile;

		output_pyfile(object _file)
		: output_c_file(fopencookie(this, "w", funcs), true), 
			pyfile(_file) { }

		static cookie_io_functions_t funcs;

		static ssize_t read(void* , char* ,size_t) { return -1; }

		static ssize_t write(void* w, const char* buf,size_t len)
		{
			//std::cerr << "In write:("<<len<<")[" << buf << "]"<< std::endl;
			auto self = (output_pyfile*)w;
			object ret = self->pyfile.attr("write")(str(buf,len));
			return extract<ssize_t>(ret);
		}
		static int seek(void* ,off64_t*,int) { return -1; }
		static int close(void* w) { return 0; }

	};

	cookie_io_functions_t output_pyfile::funcs = {
		&output_pyfile::read, 
		&output_pyfile::write, 
		&output_pyfile::seek, 
		&output_pyfile::close
	};


} // namespace anonymous



BOOST_PYTHON_MODULE(dds)
{
    using namespace boost::python;

    /**********************************************
     *
     *  dds.hh
     *
     **********************************************/

    enum_<dds::stream_op>("stream_op")
    	.value("INSERT", dds::INSERT)
    	.value("DELETE", dds::DELETE)
    	;

    class_<dds::dds_record>("record")
    	.def_readwrite("sid", &dds::dds_record::sid)
    	.def_readwrite("hid", &dds::dds_record::hid)
    	.def_readwrite("sop", &dds::dds_record::sop)
    	.def_readwrite("ts", &dds::dds_record::ts)
    	.def_readwrite("key", &dds::dds_record::key)
    	.def(self_ns::repr(self_ns::self))
    	;

    enum_<dds::qtype>("qtype")
    	.value("VOID", dds::qtype::VOID)
    	.value("SELFJOIN", dds::qtype::SELFJOIN)
    	.value("JOIN", dds::qtype::JOIN)
    	;

    class_< dds::basic_query >("query", no_init)
    	.add_property("type", &dds::basic_query::type)
    	.def(self == other<dds::basic_query>())
    	.def(self != other<dds::basic_query>())
    	;

    class_< dds::self_join, bases<dds::basic_query> >("self_join", 
    	init<dds::stream_id>())
    	.add_property("param", &dds::self_join::param )
    	;

    //std_pair_to_python_converter<dds::stream_id, dds::stream_id>();
    typedef std::pair<dds::stream_id, dds::stream_id> id_pair;
    class_< id_pair >("id_pair",
    	init<dds::stream_id, dds::stream_id>())
    	.def_readwrite("first", &id_pair::first)
    	.def_readwrite("second", &id_pair::second)
    	;

    def("join", dds::join);

    class_< dds::twoway_join, bases<dds::basic_query> >("twoway_join", 
    	init< std::pair<dds::stream_id, dds::stream_id>>())
    	.def_readwrite("param", &dds::twoway_join::param )
    	;

    // Also good for std::set<source_id> !!!!
    class_< std::set<dds::stream_id> >("id_set")
    	.def("__len__", & std::set<dds::stream_id>::size )
    	.def("__iter__", iterator<std::set<dds::stream_id>>())
    	;

    class_<dds::ds_metadata>("ds_metadata")
    	.add_property("size", &dds::ds_metadata::size)
    	.add_property("mintime", &dds::ds_metadata::mintime)
    	.add_property("maxtime", &dds::ds_metadata::maxtime)
    	.add_property("minkey", &dds::ds_metadata::minkey)
    	.add_property("maxkey", &dds::ds_metadata::maxkey)
    	.add_property("stream_ids", 
    		make_function(&dds::ds_metadata::stream_ids,
    			return_value_policy<copy_const_reference>()))
    	.add_property("source_ids", 
    		make_function(&dds::ds_metadata::source_ids,
    			return_value_policy<copy_const_reference>()))
    	;

    /**********************************************
     *
     *  data_source.hh
     *
     **********************************************/

    class_<dds::data_source>("data_source")
    	.add_property("valid", &dds::data_source::valid)
    	.def("get", &dds::data_source::get, 
    		return_value_policy<copy_const_reference>())
    	.def("advance", &dds::data_source::advance)
    	;

    def("wcup_ds", dds::wcup_ds, return_value_policy<manage_new_object>());
    def("crawdad_ds", dds::wcup_ds, return_value_policy<manage_new_object>());

	class_< dds::buffered_dataset >("buffered_dataset")    
		.def("__iter__", iterator< dds::buffered_dataset >())
    	.def("size", & dds::buffered_dataset::size )
    	.def("load", &dds::buffered_dataset::load)
    	.def("analyze", &dds::buffered_dataset::analyze)
		;


    /**********************************************
     *
     *  output.hh
     *
     **********************************************/

	class_<dds::output_table,boost::noncopyable>("output_table", no_init)
		.def("set_enabled", &dds::output_table::set_enabled)
		.def("enabled", &dds::output_table::enabled)
		.def("bind", &dds::output_table::bind, args("f"))
		.def("unbind", &dds::output_table::unbind, args("f"))
		.def("unbind_all", &dds::output_table::unbind_all)
		.def("size", &dds::output_table::size)
		.def("prolog", & dds::output_table::prolog)
		.def("epilog", & dds::output_table::epilog)
		.def("emit_row", & dds::output_table::emit_row)
		;

	class_<dds::result_table, bases<dds::output_table>,boost::noncopyable>
		("result_table", no_init)
		;

	class_<dds::time_series, bases<dds::output_table>,boost::noncopyable>
		("time_series", no_init)
		;

	class_<dds::output_file, boost::noncopyable>("output_file",no_init)
		.def("bind", &dds::output_file::bind)
		.def("unbind", &dds::output_file::unbind)
		.def("unbind_all", &dds::output_file::unbind_all)
		.def("close", &dds::output_file::close)
		.def("flush", &dds::output_file::flush)

		.def("output_prolog", &dds::output_file::output_prolog)
		.def("output_row", &dds::output_file::output_row)
		.def("output_epilog", &dds::output_file::output_epilog)
		;

	enum_<dds::open_mode>("open_mode")
		.value("truncate", dds::open_mode::truncate)
		.value("append", dds::open_mode::append)
		;

	class_<dds::output_c_file, bases<dds::output_file>, boost::noncopyable>
		("output_c_file", init<const std::string&, dds::open_mode>())
		.def(init<FILE*, bool>())
		.def(init<>())
		.def("open", 
			(void (dds::output_c_file::*)(const std::string&))  
				&dds::output_c_file::open)
		.add_property("owner", 
			&dds::output_c_file::is_owner,
			&dds::output_c_file::set_owner)
		.def("path", &dds::output_c_file::path, 
			return_value_policy<copy_const_reference>())
		;

	scope().attr("output_stdout") = ptr(&dds::output_stdout);
	scope().attr("output_stderr") = ptr(&dds::output_stderr);

	/*
		This is added in this file, as a way to interface to Python
		streams.
	 */
	class_< output_pyfile, bases<dds::output_c_file>, boost::noncopyable >
		("output_pyfile", init< object >())
		//.def("__init__", raw_function(&output_pyfile::__init__))
		.def_readonly("pyfile", &output_pyfile::pyfile)
		;


    /**********************************************
     *
     *  eca.hh
     *
     **********************************************/



    /**********************************************
     *
     *  method.hh
     *
     **********************************************/

	class_< dds::context, boost::noncopyable >("context")
		.def("run", &dds::context::run)
		.def("metadata", &dds::context::metadata,
			return_value_policy<copy_const_reference>() )
		.def_readonly("timeseries", &dds::context::timeseries)
		.def("open", 
			// a big typecast !
			(dds::output_file* 
			(dds::context::*)(const std::string&, dds::open_mode))  
						&dds::context::open, return_internal_reference<>())
		.def("close_result_files", &dds::context::close_result_files)
		;


	class_< dds::reactive, boost::noncopyable>("reactive")
		;

	//
	// Reactive components
	//

	class_< dds::dataset, boost::noncopyable>("dataset")
		.def("load", __wrap_dataset_load)
		.def("set_max_length", &dds::dataset::set_max_length)
		.def("hash_streams", &dds::dataset::hash_streams)
		.def("hash_sources", &dds::dataset::hash_sources)
		.def("set_time_window", &dds::dataset::set_time_window)
		.def("create", &dds::dataset::create)
		;

	class_<dds::reporter, bases<dds::reactive>, 
		boost::noncopyable>("reporter",
			init< size_t >())
		;

	class_<dds::progress_reporter, 
		bases<dds::reactive>, 
		boost::noncopyable>("progress_reporter",
			init< size_t >())
		;

	scope().attr("CTX") = ptr(&dds::CTX);

    /**********************************************
     *
     *  accurate.hh
     *
     **********************************************/

	// Methods
	class_<dds::data_source_statistics, 
			bases<dds::reactive>,
			boost::noncopyable
			>
		("data_source_statistics", init<>())
		;

	class_<dds::selfjoin_exact_method, 
			bases<dds::reactive>,
			boost::noncopyable
			>
		("selfjoin_exact_method", init<dds::stream_id>())
		.def("query", &dds::selfjoin_exact_method::query,
			return_value_policy<copy_const_reference>())
		.add_property("current_estimate", 
			&dds::selfjoin_exact_method::current_estimate)
		;

	class_<dds::twoway_join_exact_method, 
			bases<dds::reactive>,
			boost::noncopyable
			>
		("twoway_join_exact_method", init<dds::stream_id, dds::stream_id>())
		.def("query", &dds::twoway_join_exact_method::query,
			return_value_policy<copy_const_reference>())
		.add_property("current_estimate", 
			&dds::twoway_join_exact_method::current_estimate)
		;

}

