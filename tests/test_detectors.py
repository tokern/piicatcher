from typing import Optional, Type

from dbcat.catalog import CatColumn
from dbcat.catalog.pii_types import PiiType

from piicatcher.detectors import MetadataDetector, detector_registry, register_detector


def test_add_detector():
    class TestDetector(MetadataDetector):
        name = "test_detector"

        def detect(self, column: CatColumn) -> Optional[Type[PiiType]]:
            pass

    register_detector(TestDetector)
    assert detector_registry.get("test_detector") == TestDetector
    assert "test_detector" in detector_registry.get_all()


def test_default_detectors():
    from piicatcher import scanner  # noqa: F401

    assert "DatumRegexDetector" in detector_registry.get_all()
    assert "ColumnNameRegexDetector" in detector_registry.get_all()
