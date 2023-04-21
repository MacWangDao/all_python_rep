# cython:language_level=3
#python setup.py build_ext --inplace
#cythonize -a -i lob_c_main.pyx
from __future__ import print_function

from kafka_lob_main import start_multiproces_topics_run

def run()-> None:
    start_multiproces_topics_run()
