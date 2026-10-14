import pandas as pd
import pytest
from modules.generic_processing.structures import (
    handle_grist_null_references,
    handle_grist_nullable_int_columns,
)


def test_handle_grist_nullable_int_columns_casts_to_nullable_int() -> None:
    df = pd.DataFrame({"id_projet": [1.0, None, 3]})

    result = handle_grist_nullable_int_columns(df=df, columns=["id_projet"])

    assert str(result["id_projet"].dtype) == "Int64"
    assert result["id_projet"].tolist() == [1, pd.NA, 3]


def test_handle_grist_null_references_replaces_zero_with_na() -> None:
    df = pd.DataFrame({"id_projet": [1.0, 0, None]})

    result = handle_grist_null_references(df=df, columns=["id_projet"])

    assert str(result["id_projet"].dtype) == "Int64"
    assert result["id_projet"].tolist() == [1, pd.NA, pd.NA]


def test_handle_grist_null_references_keep_zero_when_requested() -> None:
    df = pd.DataFrame({"id_projet": [1.0, 0, None]})

    result = handle_grist_null_references(df=df, columns=["id_projet"], keep_zero=True)

    assert str(result["id_projet"].dtype) == "Int64"
    assert result["id_projet"].tolist() == [1, 0, pd.NA]


def test_handle_grist_nullable_int_columns_raises_on_non_integer_values() -> None:
    df = pd.DataFrame({"id_projet": [1.0, 2.5]})

    with pytest.raises(ValueError, match="non-integer"):
        handle_grist_nullable_int_columns(df=df, columns=["id_projet"])
