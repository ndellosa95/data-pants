from typing import Any

import pytest
from pants.engine.target import InvalidFieldException

from .third_party_package import DbtThirdPartyPackage


@pytest.mark.parametrize(
	argnames=("spec", "expected"),
	argvalues=[
		({"package": "fake"}, True),
		({"git": "fake"}, True),
		({"tarball": "fake"}, True),
		({"local": "fake"}, False),
	],
	ids=("Package", "Git", "Tarball", "Local"),
)
def test_is_third_party_package_spec(spec: dict[str, Any], expected: bool) -> None:
	assert DbtThirdPartyPackage.is_third_party_package_spec(spec) == expected


@pytest.mark.parametrize(
	argnames=("spec", "expected"),
	argvalues=[
		({"name": "fake_project", "tarball": "fake"}, "fake_project"),
		({"package": "some/fake_package"}, "fake_package"),
		({"git": "https://github.com/a/git/project.git"}, "a/git/project"),
		pytest.param({"tarball": "fake"}, "", marks=pytest.mark.raises(exception=InvalidFieldException)),
	],
	ids=("Tarball", "Package", "Git", "Malformed"),
)
def test_construct_name_from_spec(spec: dict[str, Any], expected: str) -> None:
	assert DbtThirdPartyPackage.construct_name_from_spec(spec) == expected
