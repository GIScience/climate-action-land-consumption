from pathlib import Path

import pandas as pd
from climatoology.base.artifact import _Artifact, ArtifactModality

from land_consumption.artifact import build_table_artifact


def test_build_table_artifact(compute_resources):
    input = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption', 'Total'],
            'Total Land Area [ha]': [20, 80, 100],
            '% Land Area': [20, 80, 100],
            'Consumption Factor': [1, None, None],
            '% Land Consumed by known classes': [20, None, 20],
        }
    )
    expected_output = _Artifact(
        name='Basic Report',
        modality=ArtifactModality.TABLE,
        file_path=Path(compute_resources.computation_dir / 'table_landconsumption_objects.csv'),
        summary='The proportion of land consumed by different land uses, weighted by soil sealing factors.',
        description=Path('resources/info/description.md').read_text(),
        primary=True,
    )
    computed_output = build_table_artifact(
        data=input, primary=True, resources=compute_resources, title='Basic Report', filename_suffix='objects'
    )

    assert computed_output == expected_output
