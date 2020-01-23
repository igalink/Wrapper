from cython.operator cimport dereference as deref

from libcpp.memory cimport unique_ptr
from libcpp.string cimport string

# Create a Cython extension type which holds a C++ instance
# as an attribute and create a bunch of forwarding methods
# Python extension type.
from DBEngine cimport DBEngine

#def pyExecute(query):
#    return Execute(query)

cdef class PyDbEngine:
    cdef DBEngine* c_dbe  #Hold a C++ instance which we're wrapping

    def __cinit__(self, path):
        self.c_dbe = DBEngine.Create(path)

    def __dealloc__(self):
        self.c_dbe.Reset()
        del self.c_dbe

    def execute(self, query):
        self.c_dbe.Execute(query)
