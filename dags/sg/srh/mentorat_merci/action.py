import pandas as pd
from utils.config.dag_params import get_execution_date


# Export des données
def generate_output(
    context: dict,
    file_name: str,
    df_input_mentor: pd.DataFrame,
    df_input_mentores: pd.DataFrame,
    df_ouput: pd.DataFrame,
) -> None:
    execution_date = get_execution_date(context=context)

    with pd.ExcelWriter(
        path=f"binomes_mentorat_{file_name}_{execution_date.strftime(format="v%Y-%m-%d_%Hh%M")}.xlsx"
    ) as writer:
        df_input_mentor.to_excel(excel_writer=writer, sheet_name="Mentors")
        df_input_mentores.to_excel(excel_writer=writer, sheet_name="Mentores")
        df_ouput.to_excel(excel_writer=writer, sheet_name="Binomes")


def send_result() -> None:
    pass


def generer_binome(df_mentor: pd.DataFrame, df_mentore: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    return df
