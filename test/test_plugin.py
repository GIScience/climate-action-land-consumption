import pytest
from climatoology.base.baseoperator import _Artifact
from climatoology.base.info import _Info


def test_plugin_info_request(default_operator):
    assert isinstance(default_operator.info(), _Info)


@pytest.mark.vcr
def test_plugin_compute_request(
    default_operator,
    expected_compute_input,
    compute_resources,
    default_aoi,
    default_aoi_properties,
    mock_get_osm_from_parquet,
):
    computed_artifacts = default_operator.compute(
        resources=compute_resources,
        aoi=default_aoi,
        aoi_properties=default_aoi_properties,
        params=expected_compute_input,
    )

    assert len(computed_artifacts) == 3
    for artifact in computed_artifacts:
        assert isinstance(artifact, _Artifact)
