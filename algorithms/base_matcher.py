from abc import ABC, abstractmethod


class BaseMatcher(ABC):

    @abstractmethod
    def get_matches(self, *args):
        """
        Get the column matches from a schema matching algorithm

        Parameters
        ---------
        args : Column
            The first column

        Returns
        -------
        dict
            a dictionary with matches and their similarity
        """
        raise NotImplementedError
