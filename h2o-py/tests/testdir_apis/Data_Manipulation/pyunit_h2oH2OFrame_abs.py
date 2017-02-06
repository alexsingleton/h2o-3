from __future__ import print_function
import sys
sys.path.insert(1,"../../../")
from tests import pyunit_utils
import h2o
from h2o.utils.typechecks import assert_is_type
from h2o.frame import H2OFrame

def h2o_H2OFrame():
    """
    Python API test: h2o.frame.H2OFrame.abs()
    """
    try:
        h2o.connect()
        python_lists = [[-1,2,-3],[-4,-5,-6]]
        h2oframe = h2o.H2OFrame(python_obj=python_lists)
        newframe = h2oframe.abs()       # new H2O frame contains only positive elements
        print("wowo")
    except Exception as e:
        assert False, "h2o.H2OFrame.abs() command is not working."

if __name__ == "__main__":
    pyunit_utils.standalone_test(h2o_H2OFrame())
else:
    h2o_H2OFrame()
