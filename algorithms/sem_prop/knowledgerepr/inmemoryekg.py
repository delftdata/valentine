from algorithms.sem_prop.knowledgerepr import EKGapi


class InMemoryEKG(EKGapi):

    def __init__(self, config=None):
        self.backend_type = EKGapi.BackEndType.IN_MEMORY