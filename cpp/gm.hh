#ifndef __GM_HH__
#define __GM_HH__

#include <string>

#include "dds.hh"
#include "method.hh"
#include "cfgfile.hh"

//
// Code for general geometric monitoring
//


namespace gm {

using namespace std;
using namespace dds;



template <template <qtype QType> class GMProto >
class p_component_type : public dds::basic_component_type
{
public:

	p_component_type(const string& _name) : dds::basic_component_type(_name) {}

	component* create(const Json::Value& js) override ;
};


//
// Component type declarations
//
namespace sgm {
	template <qtype QType> struct network;
	extern p_component_type<network> sgm_comptype;
}

namespace fgm {
	template <qtype QType> struct network;
	extern p_component_type<network> fgm_comptype;
}

} // end namespace gm


#endif