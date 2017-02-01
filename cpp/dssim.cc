
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
	data_source* wcup = crawdad_ds("/home/vsam/src/datasets/wifi_crawdad_sorted");
	//data_source* wcup = wcup_ds("/home/vsam/src/datasets/wc_day44");
	data_source* fds = filtered_ds(wcup, 
		FSEQ | max_length(10000) |
		modulo_attr(&dds_record::hid, (source_id)4) );

	buffered_dataset dataset;
	dataset.consume(fds);

	buffered_data_source* src = new buffered_data_source(dataset);
	data_source * wsrc = time_window(src, 100);

	std::vector<reactive*> components;
	std::vector<stream_id> sids;
	std::copy(src->metadata().stream_ids().begin(),
		src->metadata().stream_ids().end(),
		back_inserter(sids));

	for(size_t i=0; i<sids.size(); i++) {
		components.push_back(new selfjoin_exact_method(sids[i]));
		for(size_t j=i; j>0; j--)
			components.push_back(new twoway_join_exact_method(sids[j-1],sids[i]));
	}

	data_source_statistics stat;

	data_feeder feeder(wsrc);

	basic_control ctrl;
	emit(INIT);
	CTX.run();
	delete wsrc;
	for(auto p : components)
		delete p;
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



