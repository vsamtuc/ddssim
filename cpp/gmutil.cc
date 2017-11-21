
#include "gmutil.hh"

using namespace gm;
using namespace dds;

safezone::safezone() 
: szone(nullptr), inc(nullptr)
{ }

// valid safezone
safezone::safezone(safezone_func_wrapper* sz)
: szone(sz), inc(nullptr)
{
	assert(sz != nullptr);
}

safezone::~safezone()
{
	clear_inc();
}

// Movable
safezone::safezone(safezone&& other)
{
	swap(other);
}

safezone& safezone::operator=(safezone&& other)
{
	swap(other);
	return *this;
}

// Copyable
safezone::safezone(const safezone& other) 
	: inc(nullptr)
{
	szone = other.szone;
}

safezone& safezone::operator=(const safezone& other)
{
	if(szone != other.szone) {
		clear_inc();
		szone = other.szone;
	}
	return *this;
}

