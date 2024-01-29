
import pandas as pd
import numpy as np

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

#Cleaning: 

## Clean column names, remove leading and trailing space, replace leftover spaces with underscore
## The data was corrupted all newark banks are in fact from Jersey City -> column City
## All banks from Kansas are irrelevant -> colum State


@custom
def transform_custom(df: pd.DataFrame,*args, **kwargs) -> pd.DataFrame:

    # Clean column names 

    # remove spaces at begining and end
    df.columns = df.columns.str.strip()
    # replace spaces with underscore
    df.columns = df.columns.str.replace(' ','_')

    #replace newarkbanks

    df["City"] = np.where(df["City"] == "Newark", "Jersey City", df["City"])

    #drop rows with 10538 Fund

    df = df[df['State']!= "KS"]

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'