'''
Created on Jun 10, 2013

@author: pvicente
'''
from StringIO import StringIO as _StringIO
import binascii
import datetime
import struct

try:
    import json
except ImportError:
    import simplejson as json

__all__ =['encode_notifications', 'decode_feedback']

class StringIO(_StringIO):
    """Add context management protocol to StringIO
            ie: http://bugs.python.org/issue1286
    """
    
    def __enter__(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        return self
    
    def __exit__(self, exc, value, tb):
        self.close()

def encode_notifications(tokens, notifications):
    """ Returns the encoded bytes of tokens and notifications
    
                tokens                    a list of tokens or a string of only one token
                notifications     a list of notifications or a dictionary of only one
    """
    encoding_fmt = "!BH32sH%ds"
    structify = lambda t, p: struct.pack(encoding_fmt % len(p), 0, 32, t, len(p), p)
    binaryify = lambda t: t.decode('hex')
    if type(notifications) is dict and type(tokens) in (str, unicode):
        tokens, notifications = ([tokens], [notifications])
    if type(notifications) is list and type(tokens) is list:
        return ''.join(map(lambda y: structify(*y), ((binaryify(t), json.dumps(p, separators=(',',':'), ensure_ascii=False).encode('utf-8'))
                                                                        for t, p in zip(tokens, notifications))))

def decode_feedback(binary_tuples):
    """ Returns a list of tuples in (datetime, token_str) format 
    
                binary_tuples     the binary-encoded feedback tuples
    """
    decoding_fmt = '!lh32s'
    size = struct.calcsize(decoding_fmt)
    with StringIO(binary_tuples) as f:
        return [(datetime.datetime.fromtimestamp(ts), binascii.hexlify(tok))
                        for ts, toklen, tok in (struct.unpack(decoding_fmt, tup) 
                                                            for tup in iter(lambda: f.read(size), ''))]
