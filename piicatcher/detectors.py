import inspect
from abc import ABC, abstractmethod
from typing import Optional, Type

import catalogue
from dbcat.catalog.models import CatColumn
from dbcat.catalog.pii_types import PiiType


class Detector(ABC):
    """Scanner abstract class that defines required methods"""

    name: str

    pass


class MetadataDetector(Detector):
    @abstractmethod
    def detect(self, column: CatColumn) -> Optional[PiiType]:
        """Scan the text and return an array of PiiTypes that are found"""


class DatumDetector(Detector):
    @abstractmethod
    def detect(self, column: CatColumn, datum: str) -> Optional[PiiType]:
        """Scan the text and return an array of PiiTypes that are found"""


detector_registry = catalogue.create("piicatcher", "detectors", entry_points=True)


def register_detector(detector: Type["Detector"]) -> Type["Detector"]:
    """Register a detector for use.

    You can use ``register_detector(NewDetector)`` after your detector definition to automatically
    register it.
    .. code:: pycon

        >>> import piicatcher
        >>> class NewDetector(piicatcher.detectors.Detector):
        ...     pass
        >>> piicatcher.detectors.register_detector(NewDetector)
        <class 'piicatcher.detectors.catalogue.NewDetector'>

    :param detector: The ``Detector`` to register with the scrubadub detector configuration.
    :type detector: Detector class
    """
    if not inspect.isclass(detector):
        raise ValueError("detector should be a class, not an instance.")

    detector_registry.register(detector.name, func=detector)

    return detector
