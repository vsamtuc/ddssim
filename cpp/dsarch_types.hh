#ifndef __DSARCH_TYPES_HH__
#define __DSARCH_TYPES_HH__

/**
	\file Distributed stream system architecture simulation classes.

	The purpose of these classes is to collect detailed statistics
	that are independent of the particular algorithm, and report
	them in a standardized (and therefore auto-processable) manner.
  */


#include "dsarch_types.hh"

namespace dsarch {


/*-----------------------------------

	Byte sizes for the middleware

  -----------------------------------*/


/**
	By default, types with a "byte_size" method are handled.
  */
template <typename MsgType>
size_t byte_size(const MsgType& m)
{
	return m.byte_size();
}

/**
	Byte size of types, when serialized for transmission
  */
template <>
inline size_t byte_size<std::string>(const std::string& s)
{
	return s.size();
}

#define BYTE_SIZE_SIZEOF(type)\
template<>\
inline size_t byte_size<type>(const type& i) { return sizeof(type); }

BYTE_SIZE_SIZEOF(int)
BYTE_SIZE_SIZEOF(unsigned int)
BYTE_SIZE_SIZEOF(long)
BYTE_SIZE_SIZEOF(unsigned long)
//BYTE_SIZE_SIZEOF(size_t)

BYTE_SIZE_SIZEOF(float)
BYTE_SIZE_SIZEOF(double)



}


#endif