from abc import ABC, abstractmethod


class BaseMatcher(ABC):
    """
    An abstract class that is the base of a schema matching algorithm

    Methods
    -------
    get_matches(source_data_loader, target_data_loader, dataset_name)
        When implemented it should return a dictionary with matches and their similarity in descending order
    """

    @abstractmethod
    def get_matches(self, source_data_loader, target_data_loader, dataset_name):
        """
        Get the column matches from a schema matching algorithm

        Parameters
        ---------
        source_data_loader: BaseLoader
            The data loader for the source that contains either the schema, the instances or both

        target_data_loader: BaseLoader
            The data loader for the source that contains either the schema, the instances or both

        dataset_name: str
            The name of the dataset

        Returns
        -------
        dict
            A dictionary with matches and their similarity
        """
        raise NotImplementedError
