from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class StructuredEtl(ABC):
    @abstractmethod
    def read(self) -> tuple[DataFrame, ...]:
        pass

    @abstractmethod
    def transform(self, *df: DataFrame) -> tuple[DataFrame, ...]:
        pass

    @abstractmethod
    def persist(self, df: DataFrame) -> None:
        pass

    @abstractmethod
    def run(self) -> None:
        pass
