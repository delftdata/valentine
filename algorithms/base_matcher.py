from abc import ABC, abstractmethod


class BaseMatcher(ABC):

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
            a dictionary with matches and their similarity
        """
        raise NotImplementedError
