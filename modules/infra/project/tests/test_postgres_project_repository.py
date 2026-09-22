from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import UUID

import pandas as pd
import pytest

from modules.domain.projet.model import Contact, Documentation, ProjetMetadata, ProjetS3
from modules.infra.project.postgres import PostgresProjectRepository


def _make_repo(mock_db: MagicMock) -> PostgresProjectRepository:
    return PostgresProjectRepository(db=mock_db)


class TestValidation:
    @pytest.mark.parametrize(
        "method",
        ["get_list_contact", "get_list_documentation", "get_projet_s3_info", "get_projet_metadata"],
    )
    @pytest.mark.parametrize("empty", ["", None])
    def test_empty_nom_projet_raises(self, method: str, empty: object) -> None:
        mock_db = MagicMock()
        repo = _make_repo(mock_db)

        with pytest.raises(ValueError, match="nom_projet is required"):
            getattr(repo, method)(nom_projet=empty)  # type: ignore[arg-type]

        mock_db.fetch_df.assert_not_called()
        mock_db.fetch_one.assert_not_called()


class TestGetListContact:
    def test_returns_contact_list(self) -> None:
        mock_db = MagicMock()
        mock_db.fetch_df.return_value = pd.DataFrame(
            [
                {"projet": "p1", "contact_mail": "a@x.fr", "is_mail_generic": False},
                {"projet": "p1", "contact_mail": "b@x.fr", "is_mail_generic": True},
            ]
        )
        repo = _make_repo(mock_db)

        result = repo.get_list_contact(nom_projet="p1")

        assert result == [
            Contact(projet="p1", contact_mail="a@x.fr", is_mail_generic=False),
            Contact(projet="p1", contact_mail="b@x.fr", is_mail_generic=True),
        ]

        kwargs = mock_db.fetch_df.call_args.kwargs
        assert "projet_contact_vw" in kwargs["query"]
        assert kwargs["parameters"] == ("p1",)

    def test_empty_result_returns_empty_list(self) -> None:
        mock_db = MagicMock()
        mock_db.fetch_df.return_value = pd.DataFrame()
        repo = _make_repo(mock_db)

        assert repo.get_list_contact(nom_projet="p1") == []


class TestGetListDocumentation:
    def test_returns_documentation_list(self) -> None:
        mock_db = MagicMock()
        mock_db.fetch_df.return_value = pd.DataFrame(
            [{"projet": "p1", "type_documentation": "pipeline", "lien": "https://x"}]
        )
        repo = _make_repo(mock_db)

        result = repo.get_list_documentation(nom_projet="p1")

        assert result == [Documentation(projet="p1", type_documentation="pipeline", lien="https://x")]

        kwargs = mock_db.fetch_df.call_args.kwargs
        assert "projet_documentation_vw" in kwargs["query"]
        assert kwargs["parameters"] == ("p1",)


class TestGetProjetS3Info:
    def test_returns_projet_s3(self) -> None:
        mock_db = MagicMock()
        mock_db.fetch_df.return_value = pd.DataFrame([{"projet": "p1", "bucket": "bkt", "key": "k", "key_tmp": "kt"}])
        repo = _make_repo(mock_db)

        result = repo.get_projet_s3_info(nom_projet="p1")

        assert result == ProjetS3(projet="p1", bucket="bkt", key="k", key_tmp="kt")

        kwargs = mock_db.fetch_df.call_args.kwargs
        assert "projet_s3_vw" in kwargs["query"]
        assert kwargs["parameters"] == ("p1",)

    def test_empty_df_raises(self) -> None:
        mock_db = MagicMock()
        mock_db.fetch_df.return_value = pd.DataFrame()
        repo = _make_repo(mock_db)

        with pytest.raises(ValueError, match="No S3 configuration found for project p1"):
            repo.get_projet_s3_info(nom_projet="p1")


class TestGetProjetMetadata:
    def test_returns_metadata(self) -> None:
        snapshot_id = UUID("11111111-1111-1111-1111-111111111111")
        snapshot_id_parent = UUID("22222222-2222-2222-2222-222222222222")
        ts = datetime(2026, 1, 1, 12, 0, 0)
        mock_db = MagicMock()
        mock_db.fetch_one.return_value = {
            "id_projet": 7,
            "snapshot_id": snapshot_id,
            "snapshot_id_parent": snapshot_id_parent,
            "import_timestamp": ts,
        }
        repo = _make_repo(mock_db)

        result = repo.get_projet_metadata(nom_projet="p1")

        assert result == ProjetMetadata(
            _id_projet=7,
            _snapshot_id=snapshot_id,
            _snapshot_id_parent=snapshot_id_parent,
            _import_timestamp=ts,
        )
        assert result.id_projet == 7
        assert result.snapshot_id == snapshot_id
        assert result.snapshot_id_parent == snapshot_id_parent
        assert result.import_timestamp == ts

        args, kwargs = mock_db.fetch_one.call_args
        assert "versioning.snapshot" in args[0]
        assert kwargs["parameters"] == {"nom_projet": "p1", "is_dag_completed": False}

    def test_dag_completed_flag_is_forwarded(self) -> None:
        mock_db = MagicMock()
        mock_db.fetch_one.return_value = {
            "id_projet": 1,
            "snapshot_id": UUID("11111111-1111-1111-1111-111111111111"),
            "snapshot_id_parent": None,
            "import_timestamp": datetime(2026, 1, 1),
        }
        repo = _make_repo(mock_db)

        repo.get_projet_metadata(nom_projet="p1", dag_completed=True)

        _, kwargs = mock_db.fetch_one.call_args
        assert kwargs["parameters"]["is_dag_completed"] is True

    def test_no_row_raises(self) -> None:
        mock_db = MagicMock()
        mock_db.fetch_one.return_value = None
        repo = _make_repo(mock_db)

        with pytest.raises(ValueError, match="No metadata found for project p1"):
            repo.get_projet_metadata(nom_projet="p1")


def test_default_db_factory_uses_default_conn_id() -> None:
    from modules.constants import DEFAULT_PG_DATA_CONN_ID
    from modules.infra.database.factory import DatabaseType

    with patch("modules.infra.database.factory.create_db_handler") as mock_create:
        mock_create.return_value = MagicMock()
        PostgresProjectRepository()

    mock_create.assert_called_once()
    assert mock_create.call_args.kwargs["db_type"] is DatabaseType.POSTGRES
    assert mock_create.call_args.kwargs["db_config"].connection_id == DEFAULT_PG_DATA_CONN_ID
