
#
#  Toolset for data source preparation
#

import h5py
import numpy as np
from bisect import bisect_left
from collections import namedtuple

dds_record_type = np.dtype([
    ('sid','i2'),
    ('hid','i2'),
    ('key','i4'),
    ('upd','i4'),
    ('ts', 'i4')
    ],
    align=True)


class metadata:
    """
    A python implementation of the metadata object in the dds
    package.
    """
    def __init__(self, length=None, 
            ts=None, te=None, 
            kmin=None, kmax=None, 
            sids=None, hids=None):
        """
        Create a new metadata object with the given fields.
        """
        self.length = length
        self.ts = ts
        self.te = te
        self.kmin = kmin
        self.kmax = kmax
        self.sids = sids
        self.hids = hids

    def clone(self,*args,**kwargs):
        """
        Return a deep copy of this metadata object.
        """
        newsid = np.array(self.sids) if self.sids is not None else None
        newhid = np.array(self.hids) if self.hids is not None else None
        return metadata(self.length, self.ts, self.te, self.kmin, self.kmax, newsid, newhid)

    # a copy is always "deep"
    __copy__ = clone
    __deepcopy__ = clone

    def hash_streams(self, nstreams):
        """
        Apply a hash on the stream ids
        """
        self.sids = np.unique(self.sids % nstreams)
        return self

    def hash_sources(self, n):
        """
        Apply a hash on the source ids
        """
        self.hids = np.unique(self.hids % n)    
        return self

    def time_shift(self, Tw):
        """
        Shift the timestamp range by Tw
        """
        self.ts += Tw
        self.te += Tw
        return self

    def merge(self, other):
        """
        Merge with another metadata object.

        The result will be the metadata object of the merged stream.
        """
        self.length += other.length
        self.ts = min(self.ts, other.ts)
        self.te = max(self.te, other.te)
        self.kmin = min(self.kmin, other.kmin)
        self.kmax = max(self.kmax, other.kmax)
        self.sids = np.unique(np.concatenate((self.sids, other.sids)))
        self.hids = np.unique(np.concatenate((self.hids, other.hids)))
        return self

    Metadata = namedtuple('Metadata',
        ('length','ts_range','key_range','streams','sources'))

    @property
    def as_tuple(self):
        """
        Return the attributes of this metadata object as a namedtuple, nice for
        printing.
        """
        return metadata.Metadata(
            self.length, 
            (self.ts, self.te), 
            (self.kmin, self.kmax), 
            self.sids, 
            self.hids)

    def __eq__(self, other):
        try:
            return (self.length==other.length and 
                self.ts == other.ts and self.te==other.te and
                self.kmin == other.kmin and self.kmax == other.kmax and
                set(self.sids) == set(other.sids) and
                set(self.hids) == set(other.hids))
        except:
            return False
    def __ne__(self, other):
        return not self==other


def analyze(data):
    """
    Return a metadata object for the given data.

    `data` should be an array in dds_record format.
    """
    m = metadata()
    m.length = data.size
    m.ts = min(data['ts'])
    m.te = max(data['ts'])
    m.kmin = min(data['key'])
    m.kmax = max(data['key'])
    m.sids = np.unique(data['sid'])
    m.hids = np.unique(data['hid'])
    assert m
    return m


#
# Unfortunately, numpy does not have a merge method to complement
# its very nice sort method. However, sorting turns out to be really
# slow... Therefore, to speed up on the alternative
# of 'concatenate and sort', we implement a multi-merge.
#
def merge_data(*D):
    """
    Merge a number of data arrays.

    The input is a sequence of data arrays. The function returns the merging
    of all of them.
    """

    from bisect import bisect_left

    # Remove empty arrays 
    D = [d for d in D if d.size>0]
    N = len(D)
    if N==0: # empty input?
        return np.array([], dtype=dds_record_type)
    D = tuple(D)

    # reserve the result array
    total_size = sum(d.size for d in D)   
    Result = np.zeros(total_size, dtype=dds_record_type)

    # use the timestamp range to split the data
    Time0 = min(d[0]['ts'] for d in D)
    Time1 = max(d[-1]['ts'] for d in D)+1

    # initial split is all one big slab
    Sinit0, Sinit1 = tuple(0 for d in D), tuple(len(d) for d in D)

    # cache a list with the ts attributes, for speed
    DT = tuple(d['ts'] for d in D)

    pos = 0  # position to append the next slice to Results
    def recursive_merge(T0, T1, S0, S1):
        # Recursive function to split a merge into a sequence of
        # small merges. 
        # Invariant:  Every slice S0[d]:S1[d] for d in [0:N) contains 
        # only records with timestamps in the range [T0,T1)
        nonlocal pos, Result, DT, D, N

        # compute slice sizes
        sizes = [b-a for a,b in zip(S0, S1)]

        SZ = sum(sizes)
        if sizes.count(0)>=N-1:
            # only one non-empty slice, just append it to Result
            # this is important, since the merged timestamp ranges
            # may not overlap, so we should get some huge chunks
            # processed by this case, cutting the recursion early!
            for d in range(N):
                if sizes[d]:
                    Result[pos:pos+SZ] = D[d][S0[d]:S1[d]]
                    break
            pos += SZ

        elif T1-T0 <= 1:
            # we just append the ranges to Results
            q = pos
            for d in range(N):
                if sizes[d]:
                    Result[q:q+sizes[d]] = D[d][S0[d]:S1[d]]
                    q += sizes[d]
            pos += SZ

        else:
            # split the time range in half (it will be T1-T0 >= 2!)
            Tm = (T0+T1)//2
            split = tuple(bisect_left(d, Tm) for d in DT)

            # recurse
            recursive_merge(T0, Tm, S0, split)
            recursive_merge(Tm, T1, split, S1)

    recursive_merge(Time0, Time1, Sinit0, Sinit1)
    assert pos==total_size
    return Result


def is_data_sorted(data):
    t = data['ts']
    return all(t[1:]-t[:-1] >= 0)



class ddstream_array:
    """
    Distributed Data Stream data manipulator.

    An instance of this class can transform a dataset by applying various
    transformations.
    """
    def __init__(self, data, metadata=None):
        self.data = data
        if metadata is None:
            self.metadata = analyze(data)
        else:
            self.metadata = metadata
        self.attrs = {}

    def clone(self, *args, **kwargs):
        """
        Return a deep copy of this dataset.
        """
        return ddstream_array(np.array(self.data), self.metadata.clone())
    __deepcopy__ = clone
    # let __copy__ be done by default

    def hash_streams(self, nstreams):
        """
        Transform the dataset by hashing the stream ids
        """
        self.data['sid'] %= nstreams
        self.metadata.hash_streams(nstreams)
        return self

    def hash_sources(self, n):
        """
        Transform the dataset by hashing the source ids
        """
        self.data['hid'] %= n
        self.metadata.hash_sources(n)
        return self

    def negate(self):
        """
        Negate every update count on each record
        """
        self.data['upd'] = -self.data['upd']
        return self

    def time_shift(self, Tw):
        """
        Add a constant to every timestamp
        """
        self.data['ts'] += Tw
        self.metadata.time_shift(Tw)
        return self

    def merge(self, other):
        """
        Merge another dataset into this dataset.
        """
        # self.data = np.concatenate((self.data, other.data))
        # self.data.sort(order='ts', kind='mergesort')
        self.data = merge_data(self.data, other.data)
        self.metadata.merge(other.metadata)
        return self

    def time_window(self, Tw):
        """
        Transform the dataset by applying a time window.

        In particular, the stream is constructed by adding, for
        each record r, a new record r' with its timestamp increased
        by Tw and its update count negated.
        """
        return self.merge(self.clone().time_shift(Tw).negate())


    @property
    def tstart(self):
        """
        The start time of the dataset
        """
        return self.data[0]['ts']

    @property
    def tend(self):
        """
        The end time of the dataset (timestamp of last record plus one)
        """
        return self.data[-1]['ts']+1

    @property
    def tlen(self):
        """
        The difference self.tend-self.tstart
        """
        return self.data[-1]['ts']-self.data[0]['ts']+1

    def time_index(self, dt):
        """
        Return the index corresponding to the given timestamp, relative to
        the start and end of the data.

        This function returns the index of a timestamp value t, determined
        as follows: if the data timestamps are in the range [tstart,tend), 
        and dt>=0, then t = tstart+dt, whereas if dt<0, t = tend-dt+1.

        The index i returned is such that all records before it have timestamp
        ts < t, whereas all records after it have timestamp ts>=t.
        """
        ts = self.data['ts']
        if dt<0:
            t = ts[-1]+dt+1
        else:
            t = ts[0]+dt
        return bisect_left(ts,t)


    def __len__(self):
        """
        Return the length of the dataset
        """
        return len(self.data)

    def __getitem__(self, slc):
        """
        Return a new dataset with the slice of the data defined. 

        The metadata of the new dataset is adjusted only with respect to
        the timestamp range, but the key range, stream ids and sources are
        kept the same, although some values may not appear in the data
        of the new dataset.
        """
        if not isinstance(slc,slice):
            slc = slice(slc,slc+1)
        newdata = np.array(self.data[slc])
        newmeta = self.metadata.clone()
        newmeta.ts = newdata[0]['ts']
        newmeta.te = newdata[-1]['ts']
        return ddstream_array(newdata, newmeta)


    def to_hdf(self, loc, name="dsstream", compression="gzip"):
        """
        Save this stream to an HDF5 dataset.

        `loc` must be either (a) a string denoting a path, or (b) an
        h5py.Group instance (with h5py.File being also legal).

        `name` should be a string.

        If a dataset (or other object) by this name already exists, it
        will be deleted first

        Format:
        The dataset is written as a chunked array. The metadata is added as
        attributes to this array.
        """

        if not isinstance(loc, h5py.Group):
            # treat it as a string
            loc = h5py.File(loc)
        if name in loc: del loc[name]

        ds = loc.create_dataset(name, data=self.data, compression=compression)
        ds.attrs.create("key_range", 
            np.array([self.metadata.kmin, self.metadata.kmax], dtype=np.int32))
        ds.attrs.create("ts_range", 
            np.array([self.metadata.ts, self.metadata.te], dtype=np.int32))
        ds.attrs.create("stream_ids", 
            np.array(self.metadata.sids, dtype=np.int16))
        ds.attrs.create("source_ids", 
            np.array(self.metadata.hids, dtype=np.int16))

        for aname,aval in self.attrs.items():
            if aname in ("key_range", "ts_range", "stream_ids", "source_ids"):
                from warnings import warn 
                warn("From ddstream_array.to_hdf: Ignoring user-defined attribute '"
                    +aname+"' with a value of "+str(aval))
                continue

            # try to support some standard stuff
            if isinstance(aval, (list,tuple)):
                aval = np.array(aval)

            # we may fail writing a user attribute, but we should not let this
            # stop us!!
            try:
                ds.attrs[aname] = aval
            except Exception as e:
                print("Failed to save attribute ",aname,"=",aval)

        ds.file.flush()  # make sure!
        return ds


    def __repr__(self):
        return "<%s of length %d>" %( self.__class__.__name__,self.metadata.length)


def from_hdf(loc, name="dsstream"):
    if isinstance(loc, str):
        # loc is a file path
        loc = h5py.File(loc)

    if name is None:
        # assume loc is a h5py.Dataset
        dset = loc
    else:
        # assume loc is a h5py.Group
        dset = loc[name]

    # get the data
    data = dset[:]
    if data.dtype != dds_record_type or len(data.shape)!=1:
        raise ValueError("the object read is not of the right dtype or shape")
    # get the metadata
    length = data.shape[0]
    ts, te = dset.attrs['ts_range']
    kmin, kmax = dset.attrs['key_range']
    sids = dset.attrs['stream_ids']
    hids = dset.attrs['source_ids']
    m = metadata(length=length, ts=ts, te=te, kmin=kmin, kmax=kmax, sids=sids, hids=hids)

    dssa = ddstream_array(data, m)

    for aname, aval in dset.attrs.items():
        if aname in ('ts_range','key_range','stream_ids','source_ids'):
            continue
        # skip over tables attributes
        if aname in ['CLASS','VERSION','TITLE','NROWS',
            'FIELD_0_NAME','FIELD_1_NAME','FIELD_2_NAME','FIELD_3_NAME','FIELD_4_NAME',
            'FIELD_0_FILL','FIELD_1_FILL','FIELD_2_FILL','FIELD_3_FILL','FIELD_4_FILL']:
            continue
        dssa.attrs[aname] = aval

    return dssa








############################################
# 
# Native data sources
#
############################################

wcup_native_type = np.dtype([
    ('timestamp','>u4'),
    ('clientID','>u4'),
    ('objectID','>u4'),
    ('size','>u4'),
    ('method','u1'),
    ('status','u1'),
    ('type','u1'),
    ('server','u1')
    ])

def read_wcup_native(fname):
    """ 
    Read a file of native WordCup data and return an array. 
    """
    return np.fromfile(fname, wcup_native_type, -1, '')

def from_wcup(data, sid_field='type', key_field='clientID'):
    """
    Given an array of native WorldCup data, return an array in
    dds_record format.

    The `sid_field` denotes the name of the field to be used for stream_id.
    Good values are 'type' (the default), 'method' and 'status'

    The key_field denotes the `dds_record` key field. Good values
    are 'clientID' (the default) and 'objectID'.

    `data` can be either an np.array (as created by `read_wcup_native()`) or
    a string denoting a path to a file.
    """

    filename = None
    if not isinstance(data, np.ndarray):
        assert isinstance(data, str)
        filename = data
        data = read_wcup_native(filename)

    dset = np.zeros( data.size, dtype=dds_record_type)
    dset['sid'] = data[sid_field]
    dset['hid'] = data['server']
    dset['ts'] = data['timestamp']
    dset['key'] = data[key_field]
    dset['upd'] = 1
    dsa = ddstream_array(dset)

    # add some annotations
    dsa.attrs["origin"] = filename if filename is not None else "wcup"
    dsa.attrs["sid_field"] = sid_field
    dsa.attrs["key_field"] = key_field
    return dsa



