import argparse
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd  # type: ignore
from google.cloud import bigquery


def read_process_excel(
    file_path: Union[Path, str],
    sheet_name: str,
    ctr: Optional[str] = None,
    line_idx: Optional[int] = None,
    nrows: Optional[int] = None,
) -> pd.DataFrame:
    """Find table data in Excel sheets.
    Args:
        file_path: Excel file to read
        sheet_name: name of the Excel sheet to read
        ctr: country name
        line_idx: starting row to parse
        nrows: total number of rows to parse

    Returns:
        df: DataFrame with Excel contents as str
    """
    # brexit
    if "UK" in sheet_name:
        df = pd.read_excel(
            file_path,
            sheet_name=[sheet_name],
            skiprows=line_idx,
            nrows=nrows,
            na_values="N.A",
            dtype=str,
        )
        # drop empty columns
        df = df[sheet_name].loc[:, ~df[sheet_name].columns.str.startswith("Unnamed:")]
        # drop units row
        df = df.drop([0])
        df["country_code"] = ctr

    else:
        df = pd.read_excel(
            file_path, sheet_name=[sheet_name], na_values="N.A", dtype=str
        )
        # sheets do not align
        if "Prices wo taxes" in sheet_name:
            # easy case where a single column contains only country names
            ctr_list = df[sheet_name].iloc[:, 0].dropna().values
        else:
            # tricky case where country names is intertwined with dates
            # country codes have two letters, match the rows among dates and NaNs
            re_match = df[sheet_name].iloc[:, 1].str.match("[A-Z]{2}").fillna(False)
            ctr_list = df[sheet_name].loc[re_match, "Unnamed: 1"].values
        # drops NaNs with 'coerce'
        ctr_dates = (
            pd.to_datetime(df[sheet_name].iloc[:, 1], errors="coerce").dropna()
        ).dt.date
        # grab first date as latest publication
        ctr_start_dates_idx = ctr_dates.loc[ctr_dates == ctr_dates.iloc[0]].index
        # different spaces between tables in each sheet
        skip = 8 if "Prices wo taxes" in sheet_name else 7
        ctr_end_dates_idx = np.append(
            (ctr_start_dates_idx - skip)[1:], ctr_dates.index[-1]
        )
        df_list = []
        # tuple of: (country name, table init index, table end index)
        ctr_batch = zip(ctr_list, ctr_start_dates_idx, ctr_end_dates_idx)

        # iterate over country tables
        for ctr, start_date_idx, end_date_idx in ctr_batch:
            # grab country table
            df_ctr = df[sheet_name].loc[start_date_idx - 2 : end_date_idx + 1]
            # drop empty columns
            df_ctr = df_ctr.loc[:, ~df_ctr.iloc[0].isnull()]
            # rename columns with first row text
            df_ctr = df_ctr.rename(columns=df_ctr.iloc[0])
            df_ctr["country_code"] = ctr
            # append country table skipping title and units row
            df_list.append(df_ctr.iloc[2:])
        df = pd.concat(df_list)
    return df


def data_upload(
    client: bigquery.Client,
    dataset_ref: bigquery.DatasetReference,
    table: str,
    data_path: Path,
):
    """Uploads data from files to BigQuery.
    Args:
        client: BigQuery client
        dataset_ref: dataset where tables and views are saved
        table: table name where data will be uploaded
        data_path: directory where Excel files are saved
    """

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    if table == "raw_prices_1994":
        table_id = dataset_ref.table(table)
        columns = [
            "BulNum",  # bulletin number
            "Country_ID",  # country code
            # euro-super
            "EURO HT",  # hors tax
            "EURO TTC",  # with taxes (TTC??)
            "EURO_TVA",  # VAT
            "EURO_Excise",  # excise
            # diesel
            "DIESEL HT",
            "DIESEL TTC",
            "DIESEL_TVA",
            "DIESEL_Excise",
            # heating gas oil
            "HGASOIL HT",
            "HGASOIL TTC",
            "HGASOIL_TVA",
            "HGASOIL_Excise",
            # fuel oil - sulphur less than 1%
            "RFO 1 HT",
            "RFO 1 TTC",
            "RFO 1_TVA",
            "RFO 1_Excise",
            # fuel oil - sulphur higher than 1%
            "RFO 2 HT",
            "RFO 2 TTC",
            "RFO 2_TVA",
            "RFO 2_Excise",
            # lpg - motor fuel
            "LPG 1 HT",
            "LPG 1 TTC",
            "LPG 1_TVA",
            "LPG 1_Excise",
            "taux",  # exchange rate
            "price_date",
            "Euro_Price",
        ]
        file_path = data_path / "country_and_years" / "1994_2005_extraction.xls"
        df = pd.read_excel(file_path, usecols=columns, dtype=str)

        new_columns = [
            "bulletin_number",
            "country_code",
            "euro_super_wo_taxes",
            "euro_super_with_taxes",
            "euro_super_vat",
            "euro_super_excise",
            "diesel_wo_taxes",
            "diesel_with_taxes",
            "diesel_vat",
            "diesel_excise",
            "heating_gasoil_wo_taxes",
            "heating_gasoil_with_taxes",
            "heating_gasoil_vat",
            "heating_gasoil_excise",
            "fueloil_sulfur_lt_1_wo_taxes",
            "fueloil_sulfur_lt_1_with_taxes",
            "fueloil_sulfur_lt_1_vat",
            "fueloil_sulfur_lt_1_excise",
            "fueloil_sulfur_ht_1_wo_taxes",
            "fueloil_sulfur_ht_1_with_taxes",
            "fueloil_sulfur_ht_1_vat",
            "fueloil_sulfur_ht_1_excise",
            "lpg_wo_taxes",
            "lpg_with_taxes",
            "lpg_vat",
            "lpg_excise",
            "exchange_rate",
            "date",
            "price_in_euros",
        ]
        df = df.rename(columns=dict(zip(columns, new_columns)))

        df["bulletin_number"] = df["bulletin_number"].astype(int)
        df["date"] = (pd.to_datetime(df["date"])).dt.date
        df["price_in_euros"] = df["price_in_euros"] == "True"

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    elif table == "raw_prices_2005":
        renamed_columns = {
            "Date": "date",
            "Exchange\rRate\rTo €": "exchange_rate",
            "Euro-super 95  (I)": "euro_super",
            " Gas oil automobile Automotive gas oil Dieselkraftstoff (I)": "diesel",
            " Gas oil de chauffage Heating gas oil Heizöl (II)": "heating_gasoil",
            " Fuel oil - Schweres Heizöl (III) Soufre <= 1% Sulphur <= 1% Schwefel <= 1%": "fueloil_sulfur_lt_1",
            " Fuel oil - Schweres Heizöl (III) Soufre ": "fueloil_sulfur_lt_1",
            "GPL pour moteur LPG motor fuel": "lpg",
            " Fuel oil -Schweres Heizöl (III) Soufre > 1% Sulphur > 1% Schwefel > 1%": "fueloil_sulfur_ht_1",
        }
        file_url = "https://ec.europa.eu/energy/observatory/reports/Oil_Bulletin_Prices_History.xlsx"
        sheet_table = zip(
            ["Prices wo taxes", "Prices with taxes"], ["wo_taxes", "with_taxes"]
        )

        for sheet, table_suffix in sheet_table:
            df = []
            # add country data
            df.append(read_process_excel(file_url, f"{sheet}, per CTR"))
            # add UK data
            start_idx = 5 if sheet == "Prices with taxes" else 7
            df.append(
                read_process_excel(file_url, f"{sheet}, UK", "GB", start_idx, 791)
            )
            df = pd.concat(df)
            df = df.rename(columns=renamed_columns)
            df["date"] = (pd.to_datetime(df["date"])).dt.date

            table_id = dataset_ref.table(f"{table}_{table_suffix}")
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()

    else:
        raise ValueError(f"Table {table} is not valid.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load data from the EU Weekly Oil Bulletin."
    )
    parser.add_argument(
        "--dataset",
        help="Dataset reference.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--service_acc_path",
        help="Path to JSON credentials. See https://cloud.google.com/bigquery/docs/authentication/service-account-file#python.",
        default=None,
        type=str,
    )
    parser.add_argument(
        "--data_path",
        help="Path where Excel files are located.",
        type=str,
    )
    parser.add_argument(
        "--table",
        help="Source table to load.",
        choices=[
            "raw_prices_1994",
            "raw_prices_2005",
        ],
        default=None,
        type=str,
    )
    args = parser.parse_args()

    client = (
        bigquery.Client()
        if args.service_acc_path is None
        else bigquery.Client.from_service_account_json(args.service_acc_path)
    )
    dataset_ref = bigquery.DatasetReference(client.project, args.dataset)

    if args.table is not None:
        data_upload(client, dataset_ref, args.table, Path(args.data_path))
