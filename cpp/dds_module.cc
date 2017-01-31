
#include "dds.hh"
#include "data_source.hh"

#include <boost/python.hpp>
#include <boost/python/module.hpp>
#include <boost/python/class.hpp>
#include <boost/python/copy_const_reference.hpp>
#include <boost/python/return_value_policy.hpp>
#include <boost/python/module.hpp>
#include <boost/python/def.hpp>
#include <boost/python/tuple.hpp>
#include <boost/python/to_python_converter.hpp>

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

} // namespace anonymous



BOOST_PYTHON_MODULE(dds)
{
    using namespace boost::python;

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

    class_<dds::data_source>("data_source")
    	.add_property("valid", &dds::data_source::valid)
    	.def("get", &dds::data_source::get, 
    		return_value_policy<copy_const_reference>())
    	.def("advance", &dds::data_source::advance)
    	;

    def("wcup_ds", dds::wcup_ds, return_value_policy<manage_new_object>());
    def("crawdad_ds", dds::wcup_ds, return_value_policy<manage_new_object>());

    class_< std::set<dds::stream_id> >("id_set")
    	.def("size", & std::set<dds::stream_id>::size )
    	.def("__iter__", iterator<std::set<dds::stream_id>>())
    	;

    class_<dds::ds_metadata>("ds_metadata")
    	.add_property("size", &dds::ds_metadata::size)
    	.add_property("mintime", &dds::ds_metadata::mintime)
    	.add_property("maxtime", &dds::ds_metadata::maxtime)
    	.add_property("minkey", &dds::ds_metadata::minkey)
    	.add_property("maxkey", &dds::ds_metadata::maxkey)
    	.def("stream_ids", &dds::ds_metadata::stream_ids,
    		return_value_policy<copy_const_reference>())
    	.def("source_ids", &dds::ds_metadata::source_ids,
    		return_value_policy<copy_const_reference>())
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


	class_< dds::buffered_dataset >("buffered_dataset")    
		.def("__iter__", iterator< dds::buffered_dataset >())
    	.def("size", & dds::buffered_dataset::size )
    	.def("load", &dds::buffered_dataset::load)
    	.def("analyze", &dds::buffered_dataset::analyze)
		;

}

