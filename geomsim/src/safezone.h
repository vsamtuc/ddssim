/*
 * safezone.h
 *
 *  Created on: Oct 12, 2016
 *      Author: vsam
 */

#ifndef SAFEZONE_H_
#define SAFEZONE_H_



/*
 *
 * GENERAL DEFINITIONS
 *
 */

#include <cassert>
#include <map>
#include <limits>

using std::map;


//
// A wrapper for size_t indicating a dimension
//
class Reals
{
protected:
    size_t _dim;
public:
    constexpr Reals() : _dim(1) { }
    constexpr Reals(size_t n) : _dim(n) { }
    constexpr operator size_t () const { return _dim; }
    constexpr size_t dimension() const { return _dim; }
};

inline Reals operator & (const Reals& v1, const Reals& v2) { return v1+v2; }
inline Reals operator ^ (const Reals& v, size_t n) { return Reals(v*n); }


/**
 * A projector defines a contiguous range of elements in a vector
 */

class Projector : public Reals
{
private:
    size_t _offset;
public:
    constexpr Projector() : Reals(), _offset(0) {}
    constexpr Projector(const Reals& V) : Reals(V), _offset(0) {}
    constexpr Projector(const Reals& V, size_t off) : Reals(V), _offset(off) {}

    constexpr size_t begin() const { return _offset; }
    constexpr size_t end() const { return _offset+_dim; }

    constexpr bool contains(size_t p) const { return begin() <= p && p < end(); }

    constexpr bool operator==(const Projector& proj) const {
        return begin()==proj.begin() && _dim == proj._dim;
    }
    constexpr bool operator!= (const Projector& proj) const {
        return ! ((*this) == proj);
    }

    /* Note: this is a PARTIAL order! */
    constexpr bool operator<(const Projector& proj) const { return end()<=proj.begin(); }

    constexpr size_t operator[](size_t pos) const { return pos+_offset; }
};



/**
 * Vec: a sparse vector implementation
 */



class Vec
{
protected:
    typedef map<size_t, double> container;
    container _state;

    inline void clear_subspace(const Projector& proj) {
        auto from = _state.lower_bound(proj.begin());
        auto to = _state.lower_bound(proj.end());
        _state.erase(from, to);
    }


    inline void set(size_t pos, double value) {
        if(value==0.0)
            _state.erase(pos);
        else
            _state[pos] = value;
    }

    inline void set_if_not0(size_t pos, double value) {
        if(value!=0.0)
            _state[pos] = value;
    }


public:
    typedef typename container::iterator iterator;
    typedef typename container::const_iterator const_iterator;
    Reals space;

    Vec() : space(std::numeric_limits<size_t>::max()) {}
    Vec(const Reals& V) : space(V) {}


    class accessor {
        Vec& vec;
        Projector proj;
        accessor(Vec& v, const Projector& p) : vec(v), proj(p) { }
        friend class Vec;
    public:
        const accessor& operator=(const Vec&) const;
    };

    Vec(const accessor& a);

    inline accessor operator[](const Projector& p) { return accessor(*this, p); }
    inline accessor operator[](size_t pos) { return (*this)[Projector(Reals(), pos)]; }

    inline Vec& operator=(const accessor& a) {
        Vec v = a;
        return (*this) = v;
    }


    template <typename Func>
    Vec apply(const Func& f) const {
        Vec ret(space);
        for(auto& i : _state) {
            ret.set(i.first, f(i.second));
        }
        return ret;
    }


    template <typename Func>
    Vec apply(const Func& f, const Vec& V) const {
        Vec ret(space);

        auto I1 = _state.begin();
        auto I2 = V._state.begin();

        auto E1 = _state.end();
        auto E2 = V._state.end();

        while(I1!=E1 && I2!=E2) {
            if(I1==E1)
                ret.set(I2->first, f(0.0, I2->second));
            else if(I2==E2)
                ret.set(I1->first, f(I1->second, 0.0));
            else if(I1->first < I2->first)
                ret.set(I1->first, f(I1->second, 0.0));
            else if(I1->first > I2->first)
                ret.set(I2->first, f(0.0, I2->second));
            else
                ret.set(I2->first, f(I1->second, I2->second));
        }

        return ret;
    }



};



inline const Vec::accessor& Vec::accessor::operator =(const Vec& v) const
{
    vec.clear_subspace(proj);
    for(auto i = v._state.begin(); i!=v._state.end(); i++) {
        assert(i->first < proj.dimension());
        vec._state[proj[i->first]] = i->second;
    }
    return *this;
}

inline Vec::Vec(const Vec::accessor& a) : space(a.proj)
{
    Vec& v = a.vec;
    auto from = v._state.lower_bound(a.proj.begin());
    auto to = v._state.lower_bound(a.proj.end());
    for(auto i=from; i!=to; i++) {
        _state[i->first - a.proj.begin()] = i->second;
    }
}



/**
 * State
 *
 *
 * A state is a concept of a general tensorial object which is updated incrementally.
 * State data supports scalar addition and multiplication and has a 0, therefore,
 * they form a vector space.
 *
 * Expressions for states x,y:  (a double)
 * a*x -> State
 * x + y
 *
 * x = y  copy
 * x = a  make zero
 *
 *  x*y   inner product
 *
 *  x.update(loc, val)   x[loc] += val  where loc is a set of locations
 *  x.dim() -> int   the dimension of the state
 *
 *  x.space() -> StateSpace
 */


/**
 * State space.
 *
 * An object factory for states. If V is a state space, the following are supported.
 *
 *  V.zero() -> returns a (new) zero vector.
 *  V.dim()   the dimension
 *
 *  V1*V2 -> the product space
 */

// Marker type for the zero vector
struct Zero {};

// Abstract base class
class StateSpace
{
public:
    virtual size_t dim() const = 0;
};




/**
 * A query is a concept of a functional which takes as input a (local or global) state
 * and returns a real value:
 *
 * double F(const State*)
 */
template <typename State>
class Query
{
public:
    typedef State state_t;
    double operator()(const State&) = 0;
};


/**
 * An estimator is a concept of a query, an estimated state and an error epsilon. It is responsible for
 * (a) maintaining the value of
 */


/**
 *  A safe zone is a concept of a functional that takes as input a (local or global)
 *  state and returns a real value.
 *
 *  It adds to the query in terms of how it is created: at creation time, it requires
 *  an estimator
 *
 */
class SafeZone
{
public:
    SafeZone();
    virtual ~SafeZone();
};


/**
 * This class template implements query functions and contains code
 * that relates to approximate query monitoring.
 */


#endif /* SAFEZONE_H_ */
