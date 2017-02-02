
#include <memory>
#include <cstdio>
#include <chrono>
#include <libconfig.h++>


#include "data_source.hh"
#include "method.hh"
#include "accurate.hh"
#include "output.hh"
#include "dsarch.hh"

using std::shared_ptr;
using namespace dds;


void execute()
{
	//data_source* wcup = crawdad_ds("/home/vsam/src/datasets/wifi_crawdad_sorted");
	data_source* wcup = wcup_ds("/home/vsam/src/datasets/wc_day44");
	data_source* fds = filtered_ds(wcup, 
		FSEQ 
		| max_length(100000)
		| modulo_attr(&dds_record::hid, (source_id)4) );

	data_source * wsrc = time_window(fds, 3600);

	buffered_dataset dataset;
	dataset.consume(wsrc);

	buffered_data_source* src = new buffered_data_source(dataset);

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

	CTX.ds_meta = src->metadata();
	CTX.data_feed(src);

	CTX.open(stdout);
	output_file* wcout = CTX.open("wc_tseries.dat",open_mode::truncate);
	assert(dnul);

	//CTX.timeseries.bind(sto);
	CTX.timeseries.bind(wcout);
	CTX.timeseries.emit_header_row();
	reporter repter(10000);
	progress_reporter pbar(stdout, 40, "Progress: ");

	basic_control ctrl;

	using namespace std::chrono;
	steady_clock::time_point startt = steady_clock::now();
	CTX.run();
	steady_clock::time_point endt = steady_clock::now();
	cout << "Execution time=" 
		<< duration_cast<milliseconds>(endt-startt).count()/1000.0
		<< "sec" << endl;
	
	for(auto p : components)
		delete p;
	CTX.close_result_files();
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



