# coding: utf8
import struct
from collections import namedtuple
from kafkaka.util import crc32
"""
Format	C Type	            Python type	          Standard size	Notes
x	    pad byte	        no value
c	    char	            string of length 1    1
b	    signed char	        integer	              1	(3)
B	    unsigned char	    integer	              1	(3)
?	    _Bool	            bool	              1	(1)
h	    short	            integer	              2	(3)
H	    unsigned short	    integer	              2	(3)
i	    int	                integer	              4	(3)
I	    unsigned int	    integer	              4	(3)
l	    long	            integer	              4	(3)
L	    unsigned long	    integer               4	(3)
q	    long long	        integer	              8	(2), (3)
Q	    unsigned long long	integer	              8	(2), (3)
f	    float	            float	              4	(4)
d	    double	            float	              8	(4)
s	    char[]	            string
p	    char[]	            string
P	    void *	            integer	 	            (5), (3)
"""


class CounterSeed(object):
    counter = 0

    @classmethod
    def increase(cls):
        CounterSeed.counter += 1

    @classmethod
    def get(cls):
        return CounterSeed.counter


class Link():
    "I feel a link between you and me."

    def __init__(self, host, key):
        self.host = host
        self.key = key

    def get_value(self, *args, **kwargs):
        return self.host.get_field(self.key).get_value(*args, **kwargs)

    def set(self, *args, **kwargs):
        self.host.get_field(self.key).set(*args, **kwargs)

    def set_source(self, *args, **kwargs):
        self.host.get_field(self.key).set_source(*args, **kwargs)



class Field(object):
    _fmt = ''
    _encode_order = '>'

    def __init__(self, *args, **kwargs):
        CounterSeed.increase()
        self.args = args
        self.kwargs = kwargs
        self._value = None
        self._counter = CounterSeed.get()
        self._default = None
        self._repeat = None
        self._length = None
        if 'default' in kwargs:
            self.set(kwargs.get('default'))
        # which link to other Field Type or Struct Type
        for k in ('repeat', 'length', 'source'):
            if k in kwargs:
                setattr(self, '_'+k, kwargs.get(k))

    def to_static(self):
        return self.__class__, self.args, self.kwargs

    def set(self, value):
        if self._repeat and type(value) in (tuple, dict):
            self._value = value
            self._fmt *= int(self._repeat.get_value())
        else:
            self._value = value

    def get_fmt(self):
        return self._fmt

    def get_counter(self):
        return self._counter

    def get_value(self):
        return self._value

    def unpack(self, binary, cur=0):
        size = self._get_size()
        if not self._repeat:
            if len(binary) < cur + size:
                raise RuntimeError("Not enough data left")
            fmt = self._encode_order+self.get_fmt()
            value, = struct.unpack(fmt, binary[cur:cur + size])
            self.set(value)
            cur += size
        else:
            value_set = []
            n = int(self._repeat.get_value())
            for i in xrange(n):
                if len(binary) < cur + size:
                    raise RuntimeError("Not enough data left")
                fmt = self._encode_order+self.get_fmt()
                value, = struct.unpack(fmt, binary[cur:cur + size])
                value_set.append(value)
                cur += size
            self.set(value_set)
        return binary, cur

    def _get_size(self):
        return struct.calcsize(self._fmt)


class ShortField(Field):
    _fmt = 'h'


class IntField(ShortField):
    _fmt = 'i'

    def set_source(self, value):
        self._value = len(value)


class UnsignedIntField(IntField):
    _fmt = 'I'


class LongLongField(IntField):
    _fmt = 'q'


class UnsignedCharField(Field):
    _fmt = 'B'


class CharField(Field):

    def __init__(self, *arg, **kwargs):
        super(CharField, self).__init__(*arg, **kwargs)

    def set(self, value):
        self._fmt = '%ds' % len(value)
        self._value = value

    def _get_size(self):
        if self._length is not None:
            if self._length.get_value() > 0:
                self._fmt = '%ds' % self._length.get_value()
        return struct.calcsize(self._fmt)


class Crc32Field(UnsignedIntField):

    def set_source(self, value):
        self._value = crc32(value)


class StructMetaClass(type):
    def __new__(mcs, name, bases, dct):
        if len(bases) > 1:
            raise Exception('only single inherit accepted!')
        if 'Meta' not in dct:
            # reset Meta
            class Meta():
                abstract = False
            dct['Meta'] = Meta
        elif dct['Meta'].abstract:
            return super(StructMetaClass, mcs).__new__(mcs, name, bases, dct)
        base = bases[0]
        magic_dct = {
            '_fields_order': []
        }
        for _name, _value in dct.items():
            if isinstance(_value, Field):
                magic_dct['_fields_order'].append((_name, _value.get_counter()))
                dct[_name] = _value.to_static()
            elif isinstance(_value, Struct):
                magic_dct['_fields_order'].append((_name, _value.get_counter()))
                dct[_name] = _value.to_static()
        magic_dct['_fields_order'] = [k for k, v in sorted(magic_dct['_fields_order'], key=lambda item : item[1])]
        magic_dct['T'] = namedtuple(name, ' '.join(magic_dct['_fields_order']))
        dct = dict(magic_dct.items()+dct.items())
        return super(StructMetaClass, mcs).__new__(mcs, name, bases, dct)


class Struct(object):
    class Meta():
        abstract = True
    __metaclass__ = StructMetaClass
    BYTES_ORDER = '>'  # big-endian

    def __init__(self, *args, **kwargs):
        CounterSeed.increase()
        self.args = args
        self.kwargs = kwargs
        self._counter = CounterSeed.get()
        self._fmt = ""
        self._values = []
        self._repeat = None
        if 'repeat' in kwargs:
            self._repeat = kwargs['repeat']
        self._reset()
        self._set_fields(*args, **kwargs)

    def to_static(self):
        return self.__class__, self.args, self.kwargs

    def dump2nametuple(self):
        T = self.T
        tuples = []
        for field_name in self._fields_order:
            field = self.get_field(field_name)
            if isinstance(field, Field):
                tuples.append(field.get_value())
            elif isinstance(field, Struct):
                tuples.append(field.dump2nametuple())
            elif type(field) in (tuple, list):
                temp_tuples = []
                for sub_filed in field:
                    if isinstance(sub_filed, Field):
                        temp_tuples.append(sub_filed.get_value())
                    elif isinstance(sub_filed, Struct):
                        temp_tuples.append(sub_filed.dump2nametuple())
                tuples.append(temp_tuples)
        return T(*tuples)

    def create_field(self, k):
        f_class, args, kwargs = getattr(self, k)
        v = f_class(*args, **kwargs)
        return v

    def set_field(self, k, v):
        setattr(self, '_inner_'+k, v)

    def get_field(self, k):
        return getattr(self, '_inner_'+k)

    def _reset(self):
        """
        link field to field from two ways
        do some magic
        """
        for k in self._fields_order:
            v = self.create_field(k)
            self.set_field(k, v)
        for k in self._fields_order:
            v = self.get_field(k)
            for tag in ('_repeat', '_length', "_source"):
                if getattr(v, tag, None) is not None:
                    relative_field_name = getattr(v, tag)
                    relative = self.get_field(relative_field_name)
                    setattr(v, tag, Link(self, relative_field_name))  # relink to the true object
                    setattr(relative, '_reflect'+tag, Link(self, k))  # reverse link


    def get_counter(self):
        return self._counter

    def __set_fields(self, field, field_name, arg):
        if isinstance(field, Field):
            field.set(arg)
        elif isinstance(field, Struct):
            if field._repeat:
                field_set = []
                if type(arg) in (tuple, list):
                    for d in arg:
                        duplicate_field = self.create_field(field_name)
                        if type(d) in (tuple, list):
                            duplicate_field._set_fields(*d)
                        elif type(d) is dict:
                            duplicate_field._set_fields(**d)
                        field_set.append(duplicate_field)
                    self.set_field(field_name, field_set)
                    if getattr(field, '_repeat', None) != None:
                        field._repeat.set(len(field_set))
                else:
                    raise RuntimeError('repeat filed param must be tuple or list')
            else:
                if type(arg) in (tuple, list):
                    field._set_fields(*arg)
                elif type(arg) is dict:
                    field._set_fields(**arg)
        if getattr(field, '_length', None) is not None:
            t = field._length
            t.set(len(field.get_value()))
        if getattr(field, '_reflect_source', None) is not None:
            t = field._reflect_source
            field = self.get_field(field_name)  # relocate
            if isinstance(field, Field):
                t.set_source(field.get_value())
            elif isinstance(field, Struct):
                t.set_source(field.pack2bin())
            elif type(field) in (tuple, list):
                msgs = []
                for sub_field in field:
                    if isinstance(sub_field, Field):
                        msgs.append(sub_field.get_value())
                    elif isinstance(sub_field, Struct):
                        msgs.append(sub_field.pack2bin())
                t.set_source(b''.join(msgs))

    def _set_fields(self, *args, **kwargs):
        if args:
            for i, arg in enumerate(args):
                field_name = self._fields_order[i]
                field = self.get_field(field_name)
                self.__set_fields(field, field_name, arg)
        if kwargs:
            for field_name, arg in kwargs.items():
                if field_name in self._fields_order:
                    field = self.get_field(field_name)
                    self.__set_fields(field, field_name, arg)

    def _prepare_pack2bin(self):
        fmt = []
        values = []
        for k in self._fields_order:
            field = self.get_field(k)
            if isinstance(field, Field):
                fmt.append(field.get_fmt())
                value = field.get_value()
                if type(value) in (tuple, dict):
                    values += value
                else:
                    values.append(value)
            elif isinstance(field, Struct):
                if field._repeat is not None:
                    if field._repeat.get_value() == 0:
                        continue
                s_fmt, s_values = field._prepare_pack2bin()
                fmt += s_fmt
                values += s_values
            elif type(field) in (tuple, list):
                for sub_field in field:
                    if isinstance(sub_field, Field):
                        fmt.append(sub_field.get_fmt())
                        value = sub_field.get_value()
                        if type(value) in (tuple, dict):
                            values += value
                        else:
                            values.append(value)
                    elif isinstance(sub_field, Struct):
                        s_fmt, s_values = sub_field._prepare_pack2bin()
                        fmt += s_fmt
                        values += s_values
        return fmt, values

    def pack2bin(self):
        fmt, values = self._prepare_pack2bin()
        fmt = self.BYTES_ORDER + "".join(fmt)
        values = values
        self._fmt = fmt
        self._values = values
        binary = struct.pack(fmt, *values)
        return binary

    def unpack(self, binary, cur=0):
        buf = binary
        for k in self._fields_order:
            field = self.get_field(k)
            if isinstance(field, Field):
                buf, cur = field.unpack(buf, cur)
            elif isinstance(field, Struct):
                if getattr(field, '_reflect_source', None) != None and field._repeat:
                    field_set = []
                    source_left = field._reflect_source.get_value() + cur
                    while source_left:
                        duplicate_field = self.create_field(k)
                        buf, cur = duplicate_field.unpack(buf, cur)
                        field_set.append(duplicate_field)
                        source_left -= cur
                    self.set_field(k, field_set)
                elif field._repeat:
                    field_set = []
                    n = field._repeat.get_value()
                    for i in xrange(n):
                        duplicate_field = self.create_field(k)
                        buf, cur = duplicate_field.unpack(buf, cur)
                        field_set.append(duplicate_field)
                    self.set_field(k, field_set)
                else:
                    buf, cur = field.unpack(buf, cur)
        return buf, cur

    def get_size(self):
        return struct.calcsize(self._fmt)

