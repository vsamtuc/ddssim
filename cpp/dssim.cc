
#include <memory>

#include <libconfig.h++>

#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"

#include "dsarch.hh"

using std::shared_ptr;
using namespace dds;


void execute()
{
	//data_source* src = crawdad_ds("/home/vsam/src/datasets/wifi_crawdad_sorted");
	data_source* wcup = wcup_ds("/home/vsam/src/datasets/wc_day44");
	data_source* fds = filtered_ds(wcup, 
		modulo_attr(&dds_record::hid, (source_id)4) );

	buffered_dataset dataset;
	dataset.consume(fds);

	buffered_data_source* src = new buffered_data_source(dataset);
	data_source * wsrc = time_window(src, 300);

	shared_ptr<executor> E { new executor(wsrc) };
	for(auto sid : src->metadata().stream_ids())
		E->add(new selfjoin_exact_method(sid));
	E->add(new data_source_statistics());

	E->run();
}


using libconfig::Config;

Config cfg;

int main(int argc, char** argv)
{
	if(argc>1) {
		cfg.readFile(argv[1]);
	}

	execute();
	return 0;
}



