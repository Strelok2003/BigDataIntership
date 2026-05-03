"""
BoM problem approach with pandas and recursion, where we derive
whole hierarchy for each FIN material
"""

from pathlib import Path
import pandas as pd

pd.set_option('display.max_columns', None)


BASE_DIR = Path(__file__).resolve().parent

FILE_PATH = f"{BASE_DIR}/Data/task_2_data.csv"

COLUMNS_RENAME_MAP = {
        "produced_material_y": "prod_material",
        "produced_material_production_type_y": "produced_material_release_type",
        "produced_material_release_type_y": "produced_material_production_type",
        "produced_material_quantity_y": "produced_material_quantity",
        "component_material_y": "component_material",
        "component_material_production_type_y": "component_material_production_type",
        "component_material_release_type_y": "component_material_release_type",
        "component_material_quantity_y": "component_material_quantity"
    }

FIN_COLUMN_RENAME_MAP = {
        "produced_material": "fin_material_id",
        "produced_material_production_type": "fin_material_release_type",
        "produced_material_release_type": "fin_material_production_type",
        "produced_material_quantity": "fin_production_quantity",
    }

COLUMN_SELECTION_MAP = [
    "plant_id",
    "fin_material_id",
    "fin_material_release_type",
    "fin_material_production_type",
    "fin_production_quantity",

    "produced_material",
    "produced_material_release_type",
    "produced_material_production_type",
    "produced_material_quantity",

    "component_material",
    "component_material_release_type",
    "component_material_production_type",
    "component_material_quantity",

    "year",
    "month"
]

COLUMNS_TO_BE_DROPPED = [
        'component_material_x', 'component_material_production_type_x',
        'component_material_release_type_x', 'component_material_quantity_x',
        'produced_material', 'produced_material_production_type',
        'produced_material_release_type', 'produced_material_quantity',
    ]



def get_hierarchy(fin_df: pd.DataFrame, prod_df: pd.DataFrame, level:int=0) -> pd.DataFrame:
    """
    Recursively go through each production stage so that we derive
    hieararchy for each FIN material.

    In this function we merge fin_df with prod_df to get all components
    for fin_df, since resulted dataframe might still have components,
    we recursively go through it untill first production stage. since
    we already know that there is total of 3 production stage we can set
    base condition to be satisifed after 3 recursive call.


    Args:
        fin_df (pd.DataFrame): pandas dataframe with 'produced_material_release_type' = 'FIN'
        prod_df (pd.DataFrame): pandas dataframe with 'produced_material_release_type' = 'PROD'
        level (int): level of recursion (dont pass this argument when you call this function)
        
    Returns:
        Derived production hierarchy for each FIN material (pd.Dataframe)
    """
    if level > 3:
        return pd.DataFrame()

    merged_df = fin_df.merge(prod_df
                             ,left_on=["component_material", "year", "month", "plant_id"]
                             ,right_on=["produced_material" , "year", "month", "plant_id"]
                             ,how="inner") \
                             .rename(
                                 columns=COLUMNS_RENAME_MAP
                             )

    return pd.concat([merged_df
                      , get_hierarchy(merged_df.drop(columns=COLUMNS_TO_BE_DROPPED)
                                      , prod_df
                                      , level + 1)])

    

df = pd.read_csv(FILE_PATH)

fin_df = df[(df["produced_material_release_type"] == 'FIN')] \
            .rename(columns=FIN_COLUMN_RENAME_MAP)

prod_df = df[(df["produced_material_release_type"] == 'PROD')]


final_df = get_hierarchy(fin_df, prod_df)[COLUMN_SELECTION_MAP]


print(final_df)