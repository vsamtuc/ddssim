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


template <typename GMProto>
class component_type : public basic_component_type
{
public:

	component_type(const string& _name) : dds::basic_component_type(_name) {}

	GMProto* create(const Json::Value& js) override {
		using agms::projection;

		string _name = js["name"].asString();
        stream_id _sid = js["stream"].asInt();
        projection _proj = get_projection(js);
        double _beta = js["beta"].asDouble();

        return new GMProto(_name, _sid, _proj, _beta );
	}
};


} // end namespace gm


#endif