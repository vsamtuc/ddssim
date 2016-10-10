#ifndef MASSDAL_HPP
#define MASSDAL_HPP


#include <sys/time.h>
#include <algorithm>

extern void StartTheClock(int clockNumber);
extern long StopTheClock(int clockNumber);

/*
extern int MedSelect(int, int, int[]);
extern long LMedSelect(int, int, long[]);
extern long long LLMedSelect(int, int, long long[]);
extern double DMedSelect(int, int, double[]);
extern void CheckMemory(void *);
 */

template <typename T>
inline T order_select(int k, int n, T* ptr) {
	nth_element(ptr, ptr+k, ptr+n);
	return ptr[k];
}

template <typename VecType>
inline double median(const VecType& vec) {
	size_t d=vec.size();
	double v[d];
	std::copy(vec.begin(), vec.end(), v);
	return order_select(d/2,d,v);
}


#endif
