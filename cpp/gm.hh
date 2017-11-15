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

	component* create(const Json::Value& js) override {
		using agms::projection;

		string _name = js["name"].asString();
        stream_id _sid = js["stream"].asInt();
        projection _proj = get_projection(js);
        double _beta = js["beta"].asDouble();

        qtype qt = qtype::SELFJOIN;
        if(js.isMember("query"))
        	qt = qtype_repr[js["query"].asString()];

        switch(qt) {
        	case qtype::SELFJOIN:
        		return new GMProto<qtype::SELFJOIN>(_name, _sid, _proj, _beta );
        	case qtype::JOIN:
        		return new GMProto<qtype::JOIN>(_name, _sid, _proj, _beta );
			default:
				throw std::runtime_error("Query type `"+qtype_repr[qt]+"' not supported");
        }

	}

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