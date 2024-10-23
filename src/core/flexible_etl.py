from abc import ABC, abstractmethod


class FlexibleEtl(ABC):
    @abstractmethod
    def run(self):
        pass
