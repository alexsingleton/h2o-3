# -*- encoding: utf-8 -*-
"""
Group-by operations on an H2OFrame.

:copyright: (c) 2016 H2O.ai
:license:   Apache License Version 2.0 (see LICENSE for details)
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import h2o
from h2o.expr import ExprNode
from h2o.utils.compatibility import *  # NOQA
from h2o.utils.typechecks import is_type


class GroupBy(object):
    """
    A class that represents the group by operation on an H2OFrame.

    The returned groups are sorted by the natural group-by column sort.

    :param H2OFrame fr: H2OFrame that you want the group by operation to be performed on.
    :param by: by can be a str, a list of str, an int or a list of int which denote the column name(s) or
        column index(indices) of column(s) to group on.

    Sample usage:

       >>> my_frame = ...  # some existing H2OFrame
       >>> grouped = my_frame.group_by(by=["C1", "C2"])
       >>> grouped.sum(col="X1", na="all").mean(col="X5", na="all").max()
       >>> grouped.get_frame()

    Any number of aggregations may be chained together in this manner.  Note that once the aggregation
    operations are complete, calling the GroupBy object with a new set of aggregations will yield no
    effect.  You must generate a new GrouBy object in order to apply a new aggregation on it.  In addition,
    certain aggregations are only defined for numerical or categorical columns.  An error will be thrown for
    calling aggregation on the wrong data types.

    If no arguments are given to the aggregation (e.g. "max" in the above example),
    then it is assumed that the aggregation should apply to all columns but the
    group by columns.

    The ``na`` parameter is one of ``"all"`` (include NAs), ``"ignore"``, ``"rm"`` (exclude NAs).  It provides
    instructions to the aggregations on how to treat the NA entries.

    Variance (var) and standard deviation (sd) are the sample (not population) statistics.
    """

    def __init__(self, fr, by):
        """
        Return a new ``GroupBy`` object using the H2OFrame specified in fr and the desired grouping columns
        specified in by.  The original H2O frame will be stored as member _fr.  Information on the new
        grouping of the original frame is described in a new H2OFrame in member frame.

        The returned groups are sorted by the natural group-by column sort.

        :param H2OFrame fr: H2OFrame that you want the group by operation to be performed on.
        :param by: by can be a str, a list of str, an int or a list of int which denote the column name(s) or
            column index(indices) of column(s) to group on.
        """
        self._fr = fr  # IN
        self._by = by  # IN
        self._aggs = {}  # IN
        self._res = None  # OUT

        if is_type(by, str):
            self._by = [self._fr.names.index(by)]
        elif is_type(by, list, tuple):
            self._by = [self._fr.names.index(b) if is_type(b, str) else b for b in by]
        else:
            self._by = [self._by]


    def min(self, col=None, na="all"):
        """
        Calculate the minimum of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the minimum among all numeric columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.

        """
        return self._add_agg("min", col, na)


    def max(self, col=None, na="all"):
        """
        Calculate the maximum of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the maximum among all numeric columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("max", col, na)


    def mean(self, col=None, na="all"):
        """
        Calculate the mean of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the mean among all numeric columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("mean", col, na)


    def count(self, na="all"):
        """
        Count the number of rows in each group of a GroupBy object.

        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("nrow", None, na)


    def sum(self, col=None, na="all"):
        """
        Calculate the sum of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the sum among all numeric columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("sum", col, na)


    def sd(self, col=None, na="all"):
        """
        Calculate the standard deviation of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the standard deviation among all numeric columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("sdev", col, na)


    def var(self, col=None, na="all"):
        """
        Calculate the variance of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the variance among all numeric columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("var", col, na)


    def ss(self, col=None, na="all"):
        """
        Calculate the sum of squares of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the sum of squares among all numeric columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("sumSquares", col, na)


    def mode(self, col=None, na="all"):
        """
        Calculate the mode of column(s) col within each group of a GroupBy object.  If no col is given,
        compute the mode among all categorical columns other than those being grouped on.

        :param col: col can be None (default), a str, a list of str, an int or a list of int which denote the column
            name(s) or column index(indices) of column(s) to group on.
        :param str na:  one of 'rm', 'ignore' or 'all' (default).
        :return: the original GroupBy object (self), for ease of constructing chained operations.
        """
        return self._add_agg("mode", col, na)


    @property
    def frame(self):
        """
        Return the resulting H2OFrame containing the result(s) of aggregation(s) of the group by.

        The number of rows denote the number of groups generated by the group by operation.

        The number of columns depend on the number of aggregations performed, the number of columns specified in
        the col parameter.  Generally, expect the number of columns to be
            (number of cols specified in parameter col for aggregation 0)+
            (number of cols specified in parameter col for aggregation 1)+
            ...+
            (number of cols specified in parameter col for last aggregation)+
            1 (for group-by group names).

         However, the count aggregation only generates one column.
        """
        return self.get_frame()


    def get_frame(self):
        """
        Return the resulting H2OFrame containing the result(s) of aggregation(s) of the group by.

        The number of rows denote the number of groups generated by the group by operation.

        The number of columns depend on the number of aggregations performed, the number of columns specified in
        the col parameter.  Generally, expect the number of columns to be
            (number of cols specified in parameter col for aggregation 0)+
            (number of cols specified in parameter col for aggregation 1)+
            ...+
            (number of cols specified in parameter col for last aggregation)+
            1 (for group-by group names).

         However, the count aggregation only generates one column.
        """
        if self._res is None:
            aggs = []
            for k in self._aggs: aggs += (self._aggs[k])
            self._res = h2o.H2OFrame._expr(expr=ExprNode("GB", self._fr, self._by, *aggs))
        return self._res


    def _add_agg(self, op, col, na):
        if op == "nrow": col = 0
        if col is None:
            for i in range(self._fr.ncol):
                if i not in self._by: self._add_agg(op, i, na)
            return self
        elif is_type(col, str):
            cidx = self._fr.names.index(col)
        elif is_type(col, int):
            cidx = col
        elif is_type(col, list, tuple):
            for i in col:
                self._add_agg(op, i, na)
            return self
        else:
            raise ValueError("col must be a column name or index.")
        name = "{}_{}".format(op, self._fr.names[cidx])
        self._aggs[name] = [op, cidx, na]
        return self


    def __repr__(self):
        print("GroupBy: ")
        print("  Frame: {}; by={}".format(self._fr.frame_id, str(self._by)))
        print("  Aggregates: {}".format(str(self._aggs.keys())))
        print("*** Use get_frame() to get groupby frame ***")
        return ""
