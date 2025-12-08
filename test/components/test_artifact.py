from pathlib import Path

import pandas as pd
from climatoology.base.artifact import ArtifactModality

from land_consumption.components.artifact import build_table_artifact, build_treemap_artifact
import plotly.express as px


def test_build_table_artifact(compute_resources):
    input = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Other', 'Total'],
            'Total Land Area [ha]': [20, 80, 100],
            '% of Consumed Land Area': [20, None, 100],
            '% of Settled Land Area': [20, 80, 100],
        }
    )
    expected_file_path = Path(compute_resources.computation_dir / 'table_landconsumption_basic.csv')
    computed_output = build_table_artifact(data=input, resources=compute_resources, title='basic')

    assert computed_output.modality == ArtifactModality.TABLE
    assert computed_output.name == 'Basic Report'
    assert computed_output.file_path == expected_file_path


def test_build_treemap_artifact(compute_resources):
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Buildings', 'Other'],
            'Land Use Class': ['Commercial', 'Industrial', 'Other'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% of Settled Land Area': [50.0, 25.0, 25.0],
        }
    )

    input_treemap = px.treemap(
        data,
        path=[px.Constant('Land Use Overview'), 'Land Use Object', 'Land Use Class'],
        values='Total Land Area [ha]',
        custom_data=['Land Use Object', 'Land Use Class'],
    )
    expected_file_path = Path(compute_resources.computation_dir / 'land_consumption_treemap.json')

    computed_output = build_treemap_artifact(figure=input_treemap, resources=compute_resources)

    assert computed_output.modality == ArtifactModality.CHART_PLOTLY
    assert computed_output.name == 'Land Consumption Treemap'
    assert computed_output.file_path == expected_file_path
