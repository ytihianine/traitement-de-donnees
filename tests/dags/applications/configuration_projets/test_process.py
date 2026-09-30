import pandas as pd
from dags.applications.configuration_projets.process import process_dim_projet_contact


def test_process_dim_projet_contact_keeps_id_contact_as_nullable_int() -> None:
    df_projet = pd.DataFrame(
        {
            "id_projet": [1, 2],
            "projet": ["A", "B"],
            "id_direction": [10, 11],
            "id_service": [20, 21],
        }
    )
    df_projet_contact = pd.DataFrame(
        {
            "id_projet": [1],
            "id_contact": [12],
            "contact_mail": ["a@example.org"],
            "is_mail_generic": [True],
        }
    )

    result = process_dim_projet_contact(df_projet=df_projet, df_projet_contact=df_projet_contact)

    assert str(result["id_contact"].dtype) == "Int64"
    assert result["id_contact"].tolist() == [12, pd.NA]
