from unittest.mock import Mock

import pandas as pd
import pytest
from modules.domain.dataset.model import StageLocation
from modules.infra.database.base import DBInterface
from modules.infra.database.repository.dataset_context import DbDatasetContextRepository


def _repository_with_frame(monkeypatch: pytest.MonkeyPatch, dataframe: pd.DataFrame) -> DbDatasetContextRepository:
    db_client = Mock(spec=DBInterface)
    db_client.fetch_df.return_value = dataframe
    monkeypatch.setattr(
        "modules.infra.database.repository.dataset_context.create_db_handler",
        Mock(return_value=db_client),
    )
    return DbDatasetContextRepository()


def test_get_list_returns_a_context_for_each_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    dataframe = pd.DataFrame(
        [
            {
                "id_projet": 1,
                "projet": "mon-projet",
                "id_dataset": 10,
                "dataset": "alpha",
                "stage": "Source",
                "type_location": "grist",
                "location": "alpha-source",
                "conn_id": "grist_default",
            },
            {
                "id_projet": 1,
                "projet": "mon-projet",
                "id_dataset": 10,
                "dataset": "alpha",
                "stage": "Destination",
                "type_location": "database",
                "location": "alpha-destination",
                "conn_id": "postgres_default",
            },
            {
                "id_projet": 1,
                "projet": "mon-projet",
                "id_dataset": 20,
                "dataset": "beta",
                "stage": "Source",
                "type_location": "s3",
                "location": "beta-source",
                "conn_id": "s3_default",
            },
            {
                "id_projet": 1,
                "projet": "mon-projet",
                "id_dataset": 20,
                "dataset": "beta",
                "stage": "Temporaire",
                "type_location": "local",
                "location": "beta-temporary",
                "conn_id": None,
            },
        ]
    )
    repository = _repository_with_frame(monkeypatch=monkeypatch, dataframe=dataframe)

    contexts = repository.get_list(nom_projet="mon-projet")

    assert [context.dataset_name for context in contexts] == ["alpha", "beta"]
    assert contexts[0].projet_name == "mon-projet"
    assert contexts[0].location[StageLocation.DESTINATION].location == "alpha-destination"
    assert contexts[1].location[StageLocation.SOURCE].location == "beta-source"
    assert contexts[1].location[StageLocation.TEMPORAIRE].location == "beta-temporary"


def test_get_list_raises_for_unknown_project(monkeypatch: pytest.MonkeyPatch) -> None:
    repository = _repository_with_frame(monkeypatch=monkeypatch, dataframe=pd.DataFrame())

    with pytest.raises(ValueError, match="No project found with name mon-projet"):
        repository.get_list(nom_projet="mon-projet")
