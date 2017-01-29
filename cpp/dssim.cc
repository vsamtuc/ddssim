
#include <memory>

#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"

#include "dsarch.hh"

using std::shared_ptr;
using namespace dds;


void execute()
{
	//data_source* src = crawdad_ds("/home/vsam/src/datasets/wifi_crawdad_sorted");
	data_source* src = wcup_ds("/home/vsam/src/datasets/wc_day44");

	data_source* fds = filtered_ds(src, 
		modulo_attr(&dds_record::hid, (source_id)4) );


	shared_ptr<executor> E { new executor(fds) };
	E->add(new data_source_statistics());

	E->run();
}


int main()
{
	execute();
	return 0;
}