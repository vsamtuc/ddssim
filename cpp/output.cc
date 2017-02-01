
#include <cstdio>
#include "output.hh"

using namespace dds;


void output_table::emit_header_start(FILE* stream)
{
	fputs("#INDEX",stream);
}

void output_table::emit_row_start(FILE* stream)
{
	fprintf(stream,"%s", name().c_str());
}

void time_series::emit_header_start(FILE* stream)
{
	fprintf(stream, "#%s", now.name().c_str());
}

void time_series::emit_row_start(FILE* stream)
{
	fprintf(stream,now.format(), now.value());
}

void output_table::emit_header(FILE* stream)
{
	if(!enabled()) return;
	emit_header_start(stream);
	for(auto col : columns) {
		fprintf(stream, ",%s", col->name().c_str());
	}
	fputs("\n",stream);
}

void output_table::emit(FILE* stream)
{
	if(!enabled()) return;
	emit_row_start(stream);
	for(auto col : columns) {
		fputs(",", stream);
		col->emit(stream);
	}
	fputs("\n",stream);
}

