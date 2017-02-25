
#include "dds.hh"
#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"
#include "tods.hh"
#include "geometric.hh"
#include "results.hh"
#include "binc.hh"

#include <boost/python.hpp>

using binc::print;

namespace { // Avoid cluttering the global namespace.


	// template <typename DS>
	// void __wrap_dataset_load(dds::dataset& D, 
	// 	std::auto_ptr<DS> p)
	// {
	// 	D.load(p.get());
	// 	p.release();
	// }

	// inline dds::data_source* __wrap_time_window(std::auto_ptr<dds::data_source> ds, dds::timestamp ts)
	// {
	// 	dds::data_source* ret = dds::time_window(ds.get(), ts);
	// 	ds.release();
	// 	return ret;
	// }

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


	struct pyaction : dds::action
	{
		object action;
		pyaction(object _action) : action(_action) {}
		virtual void run() override {
			action();
		}
	};
	struct pycondaction : pyaction 
	{
		object cond;
		pycondaction(object _cond, object _action)
		: pyaction(_action), cond(_cond) {}
		virtual void run() override {
			if(extract<bool>(cond()))
				action();
		}
	};

	dds::eca_rule on_pyaction(dds::basic_control* self, 
		dds::Event evt, object action)
	{
		return self->add_rule(evt, new pyaction(action));
	}
	dds::eca_rule on_pycondaction(dds::basic_control* self, 
		dds::Event evt, object cond, object action)
	{
		return self->add_rule(evt, new pycondaction(cond,action));
	}
	dds::eca_rule react_on_pyaction(dds::reactive* self, 
		dds::Event evt, object action)
	{
		return self->add_rule(evt, new pyaction(action));
	}
	dds::eca_rule react_on_pycondaction(dds::reactive* self, 
		dds::Event evt, object cond, object action)
	{
		return self->add_rule(evt, new pycondaction(cond,action));
	}

	template <typename Elem>
	struct set_ops {
		typedef std::set<Elem> Set;
		static bool not_empty(Set const& s) { return !s.empty(); }

		static bool contains(Set const& s, const Elem& e) {
			return s.find(e)!=s.end();
		}

		static void add(Set& s, const Elem& e) {
			s.insert(e);
		}

		static void union_(Set& s, boost::python::list l) {
			l[0];
		}
	};


	template<class T1, class T2>
	struct PairToTupleConverter {
	  static PyObject* convert(const std::pair<T1, T2>& p) {
	    return incref(make_tuple(p.first, p.second).ptr());
	  }
	};

    typedef std::pair<dds::stream_id, dds::stream_id> id_pair;

	struct TupleToIdPairConverter {

		TupleToIdPairConverter() {
			boost::python::converter::registry::push_back(
				&convertible,
				&construct,
				boost::python::type_id<id_pair>());
		}

		// Determine if obj_ptr can be converted in a QString
		static void* convertible(PyObject* obj_ptr)
		{
		  	if (!PyTuple_Check(obj_ptr)) return 0;
		  	if (PyTuple_Size(obj_ptr)!=2) return 0;
		  	if( !PyLong_Check(PyTuple_GetItem(obj_ptr, 0))
		  		|| !PyLong_Check(PyTuple_GetItem(obj_ptr, 0)))
		  	return 0; 
		  	return obj_ptr;
		}
 	
		static void construct(	PyObject* obj_ptr,
			converter::rvalue_from_python_stage1_data* data)
		{
			// Extract the character data from the python string
			dds::stream_id s1 = PyLong_AsLong(PyTuple_GetItem(obj_ptr,0));
			dds::stream_id s2 = PyLong_AsLong(PyTuple_GetItem(obj_ptr,1));

			// Grab pointer to memory into which to construct the new QString
			void* storage = (
			(converter::rvalue_from_python_storage<id_pair>*)
			data)->storage.bytes;

			// in-place construct the new QString using the character data
			// extraced from the python object
			new (storage) id_pair(s1, s2);

			// Stash the memory chunk pointer for later use by boost.python
			data->convertible = storage;
		}
 
	};

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(_sketch_update_overloads, update, 1, 2)

	using numeric::array;


} // namespace anonymous



BOOST_PYTHON_MODULE(_dds)
{
    using namespace boost::python;
    namespace py = boost::python;

    to_python_converter< std::pair<short, short>, 
    	PairToTupleConverter<short, short> > __dummy();

    TupleToIdPairConverter();

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
    	.def(repr(self))
    	;

    class_< dds::self_join, bases<dds::basic_query> >("self_join", 
    	init<dds::stream_id>())
    	.add_property("param", &dds::self_join::param )
    	;

    //std_pair_to_python_converter<dds::stream_id, dds::stream_id>();

    // class_< id_pair >("id_pair",
    // 	init<dds::stream_id, dds::stream_id>())
    // 	.def_readwrite("first", &id_pair::first)
    // 	.def_readwrite("second", &id_pair::second)
    // 	;

    def("join", dds::join);

    class_< dds::twoway_join, bases<dds::basic_query> >("twoway_join", 
    	init< std::pair<dds::stream_id, dds::stream_id>>())
    	//.def_readonly("param", &dds::twoway_join::param )
    	.add_property("param", make_function(
    		[](dds::twoway_join& q) {
    			return py::make_tuple(q.param.first, q.param.second);
    		},
    		return_value_policy<return_by_value>(),
    		boost::mpl::vector<py::tuple, dds::twoway_join>()
    		)
    	)
    	;

    // Also good for std::set<source_id> !!!!
    class_< std::set<dds::stream_id> >("id_set")
    	.def("__len__", & std::set<dds::stream_id>::size )
    	.def("__iter__", py::iterator<std::set<dds::stream_id>>())
    	.add_property("empty", &std::set<dds::stream_id>::empty)
    	.def("clear", &std::set<dds::stream_id>::clear )
    	.def("__bool__", &set_ops<dds::stream_id>::not_empty )
    	.def("__contains__", &set_ops<dds::stream_id>::contains )
    	.def("add", &set_ops<dds::stream_id>::add )    	
    	;

    class_<dds::ds_metadata>("ds_metadata")
    	.add_property("size", &dds::ds_metadata::size)
    	.add_property("mintime", &dds::ds_metadata::mintime)
    	.add_property("maxtime", &dds::ds_metadata::maxtime)
    	.add_property("minkey", &dds::ds_metadata::minkey)
    	.add_property("maxkey", &dds::ds_metadata::maxkey)
    	.def("set_size", &dds::ds_metadata::set_size)
    	.def("set_key_range", &dds::ds_metadata::set_key_range)
    	.def("set_ts_range", &dds::ds_metadata::set_ts_range)
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

    class_<dds::data_source, boost::noncopyable>("data_source")
    	.add_property("valid", &dds::data_source::valid)
    	.def("get", &dds::data_source::get, 
    		return_value_policy<copy_const_reference>())
    	.def("advance", &dds::data_source::advance)
    	.def("valid", &dds::data_source::valid)
		.def("__iter__", py::iterator<dds::data_source>())    	
    	;

    py::register_ptr_to_python< dds::datasrc > ();

    class_<dds::time_window_source, bases<dds::data_source>, boost::noncopyable>(
    	"time_window_source", init<dds::datasrc, dds::timestamp>())
    	.add_property("delay", & dds::time_window_source::delay)
    	;

    def("wcup_ds", dds::wcup_ds);
    def("crawdad_ds", dds::wcup_ds);
    def("time_window", dds::time_window);

    class_< dds::analyzed_data_source, bases<dds::data_source>, 
    	boost::noncopyable>(
    		"analyzed_data_source", no_init)
    	.def("metadata", &dds::analyzed_data_source::metadata,
    		return_internal_reference<>())
    	;

    class_< dds::uniform_data_source, bases<dds::analyzed_data_source>, 
    	boost::noncopyable>(
    		"uniform_data_source",
    		init<dds::stream_id, dds::source_id, dds::key_type, dds::timestamp>()
    	)
    	;

	class_< dds::buffered_dataset >("buffered_dataset")
		.def("__iter__", py::iterator< dds::buffered_dataset >())
    	.def("__len__", & dds::buffered_dataset::size )
    	.def("size", & dds::buffered_dataset::size )
    	.def("load", &dds::buffered_dataset::load)
    	.def("analyze", &dds::buffered_dataset::analyze)
		;

	class_< dds::buffered_data_source, 
			bases<dds::analyzed_data_source>,
			boost::noncopyable >("buffered_data_source",
				init<dds::buffered_dataset&>()[
					with_custodian_and_ward<1,2>()])
			.def(init<dds::buffered_dataset&,
					const dds::ds_metadata&>()[
						with_custodian_and_ward<1,2>()
					])
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


	class_< dds::output_hdf5, bases<dds::output_file>, boost::noncopyable>
		("output_hdf5", init<int, dds::open_mode>())
		.def(init<const std::string&>())
		;


    /**********************************************
     *
     *  eca.hh
     *
     **********************************************/

	class_<dds::Event>("Event", init<int>())
		.def(self == other<dds::Event>())
		.def(self < other<dds::Event>())
		.def(int_(self))
		;

	scope().attr("INIT") = dds::INIT;
	scope().attr("DONE") = dds::DONE;
	scope().attr("START_STREAM") = dds::START_STREAM;
	scope().attr("END_STREAM") = dds::END_STREAM;
	scope().attr("START_RECORD") = dds::END_RECORD;
	scope().attr("END_RECORD") = dds::END_RECORD;
	scope().attr("VALIDATE") = dds::VALIDATE;
	scope().attr("REPORT") = dds::REPORT;
	scope().attr("RESULTS") = dds::RESULTS;

	class_< dds::eca_rule >("eca_rule", 
		init<const dds::eca_rule&>())
		.def_readonly("event", &dds::eca_rule::first)
		;

	class_< dds::basic_control >("basic_control", init<>())
		.def("now", &dds::basic_control::now)
		.def("stream_record", &dds::basic_control::stream_record,
			return_value_policy<copy_const_reference>())
		.def("metadata", &dds::basic_control::metadata,
			return_value_policy<copy_const_reference>())
		.def("data_feed", &dds::basic_control::data_feed,
				with_custodian_and_ward_postcall<1,2>()
			)
		.def("run", &dds::basic_control::run)
		//.def("add_rule", &dds::basic_control::add_rule,
		//	return_value_policy<return_by_value>())
		.def("on", on_pyaction, return_value_policy<return_by_value>())
		.def("on", on_pycondaction, return_value_policy<return_by_value>())
		.def("cancel_rule", &dds::basic_control::cancel_rule)
		;



    /**********************************************
     *
     *  method.hh
     *
     **********************************************/

	class_< dds::context, bases<dds::basic_control>, boost::noncopyable >
		("context", no_init)
		.def("run", &dds::context::run)
		.def_readonly("timeseries", &dds::context::timeseries)
		.def("open", 
			// a big typecast !
			(dds::output_file* 
			(dds::context::*)(const std::string&, dds::open_mode))  
						&dds::context::open, return_internal_reference<>())
		.def("close_result_files", &dds::context::close_result_files)
		;


	class_< dds::reactive, boost::noncopyable>("reactive", init<>())
		.def("on", react_on_pyaction, return_value_policy<return_by_value>())
		.def("on", react_on_pycondaction, return_value_policy<return_by_value>())
		.def("cancel", &dds::reactive::cancel)
		.def("cancel_all", &dds::reactive::cancel_all)
		;

	//
	// Reactive components
	//

	class_< dds::dataset, boost::noncopyable>("dataset")
		// we must overload manually with all subclasses of data_source :-(
		.def("load", &dds::dataset::load)
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

	class_< dds::selfjoin_agms_method,
			bases<dds::reactive>,
			boost::noncopyable
			>
		("selfjoin_agms_method", 
			init<dds::stream_id, agms::depth_type, agms::index_type>()
		)
		.def("query", &dds::selfjoin_agms_method::query,
			return_value_policy<copy_const_reference>())
		.add_property("current_estimate", 
			&dds::selfjoin_agms_method::current_estimate)
		;


	class_< dds::twoway_join_agms_method,
			bases<dds::reactive>,
			boost::noncopyable
			>
		("twoway_join_agms_method", 
			init<dds::stream_id, dds::stream_id, 
				agms::depth_type, agms::index_type>()
		)
		.def("query", &dds::twoway_join_agms_method::query,
			return_value_policy<copy_const_reference>())
		.add_property("current_estimate", 
			&dds::twoway_join_agms_method::current_estimate)
		;


    /**********************************************
     *
     *  agms.hh
     *
     **********************************************/

	class_< agms::hash_family, boost::noncopyable>
		("agms_hash_family", init<agms::depth_type>())
		.def("hash", &agms::hash_family::hash)
		.def("fourwise", &agms::hash_family::fourwise)
		.add_property("depth", &agms::hash_family::depth)
		;

	def("agms_hash_family_get_cached", &agms::hash_family::get_cached,
		return_internal_reference<>());

	class_< agms::projection >("agms_projection", 
		init<agms::hash_family*, agms::index_type>(args("hf","width"))[
			with_custodian_and_ward<1,2>()
			])
		.def(init<agms::depth_type, agms::index_type>())
		.def("hashf", &agms::projection::hashf, return_internal_reference<>())
		.add_property("depth", &agms::projection::depth)
		.add_property("width", &agms::projection::width)
		.add_property("size", &agms::projection::size)
		.def(self == other<agms::projection>())
		.def(self != other<agms::projection>())
		.def("epsilon", &agms::projection::epsilon)
		.def("prob_failure", &agms::projection::prob_failure)
		;

	object numpy = import("numpy");
	//numeric::array::set_module_and_type("numpy","ndarray");

	class_< agms::sketch >("agms_sketch",
			init<agms::projection>())
		.def(init<agms::depth_type, agms::index_type>())
		.def("hashf", &agms::sketch::hashf, return_internal_reference<>())
		.add_property("width", &agms::sketch::width)
		.add_property("depth", &agms::sketch::depth)
		.def_readonly("proj", &agms::sketch::proj)
		.def("update", &agms::sketch::update, _sketch_update_overloads())
		.def("insert", &agms::sketch::insert)
		.def("erase", &agms::sketch::erase)
		.def("compatible", &agms::sketch::compatible)
		.def("norm2_squared", &agms::sketch::norm2_squared)
		.def("byte_size", &agms::sketch::byte_size)
		.def(self + other<agms::sketch>())
		.def(self - other<agms::sketch>())
		.def(self * other<double>())
		.def(other<double>() * self)
		;


    /**********************************************
     *
     *  results.hh
     *
     **********************************************/

	class_< dds::comm_results_t, bases<dds::result_table>, boost::noncopyable>
		("comm_results_t")
		;

	try {
		scope().attr("lsstats") = ptr(&dds::lsstats);
		scope().attr("comm_results") = py::ptr(&dds::comm_results);
	} catch(std::exception e) {
		print("Error in binding result tables",e.what());
	}


    /**********************************************
     *
     *  tods.hh
     *
     **********************************************/

	class_< dds::tods::network, bases<dds::reactive>, boost::noncopyable>
		("tods_network", init<const projection&, double>())
		.def(init<agms::depth_type, agms::index_type, double>())
		.def("maximum_error", &dds::tods::network::maximum_error)
		;		

    /**********************************************
     *
     *  geometric.hh
     *
     **********************************************/

	class_< dds::gm2::network, bases<dds::reactive>, boost::noncopyable >
		("gm2_network", init<dds::stream_id, const agms::projection&, double>())
		.def(init<dds::stream_id, agms::depth_type, agms::index_type, double>())
		;

}

