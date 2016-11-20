from distutils.core import setup
from Cython.Build import cythonize

setup(name="CSVParser",ext_modules=cythonize("FileRead.pyx"))