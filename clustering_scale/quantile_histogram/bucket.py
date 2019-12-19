import math


class Bucket(object):
    """
    A class used to represent a bucket of a histogram

                     Bucket
         |---------------------------|
    lower_bound                 upper_bound


    Attributes
    ----------
    lower_bound : int
        the lower bound of the bucket
    upper_bound : int
        the upper bound of the bucket
    contents : dict
        the dictionary containing the contents of the bucket {content: frequency}

    Methods
    -------
    create_bucket(bucket_data: list, column_size: int)
        creates a bucket with the specified data and it normalizes its frequencies based on the column size

    add_value(value: int)
        adds a value to the bucket

    set_lower_bound(lb: int)
        sets the lower bound of the bucket

    set_upper_bound(ub: int)
        sets the upper bound of the bucket

    normalize_bucket(column_size: int)
        normalizes the bucket value frequencies based on the column size
    """
    def __init__(self, bucket_data: list = None, column_size: int = 0):
        """
        Parameters
        ----------
        bucket_data : list, optional
            the data that will be added to the bucket (default None no data will be added)
        column_size : int
            the size of the column
        """
        self.lower_bound = math.inf
        self.upper_bound = -math.inf
        self.contents = dict()
        if bucket_data is not None:
            self.create_bucket(bucket_data, column_size)

    def create_bucket(self, bucket_data: list, column_size: int):
        """
        Creates a bucket with the specified data and it normalizes its frequencies based on the column size

        Parameters
        ---------
        bucket_data : list
            the data to be added
        column_size : int
            the size of the column
        """
        for value in bucket_data:
            self.lower_bound = min(self.lower_bound, value)
            self.upper_bound = max(self.upper_bound, value)
            self.contents[value] = self.contents.get(value, 0) + 1
        self.normalize_bucket(column_size)

    def add_value(self, value: int):
        """
        Adds a value to a bucket

        Parameters
        ---------
        value : int
            the value to be added
        """
        self.contents[value] = self.contents.get(value, 0) + 1

    def set_lower_bound(self, lb: int):
        """
        Sets the lower bound of a bucket

        Parameters
        ---------
        lb : int
            the new lower bound
        """
        self.lower_bound = lb

    def set_upper_bound(self, ub: int):
        """
        Sets the upper bound of a bucket

        Parameters
        ---------
        ub : int
            the new upper bound
        """
        self.upper_bound = ub

    def normalize_bucket(self, column_size: int):
        """
        Normalizes th bucket's frequencies based on the column size

        Parameters
        ---------
        column_size : int
            the column size
        """
        self.contents = {k: self.contents[k] / column_size for k, v in self.contents.items()}
