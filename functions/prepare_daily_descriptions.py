"""
    Prepare daily vehicle descriptions by aligning them with the ads selected for processing.

    Steps:
        1. Normalize null-like values in `raw_descriptions` 
           (e.g., "", " ", "nan", "NaN", "None") → converted to pd.NA.
        2. Extract the set of ad IDs from `ads`.
        3. Filter and return only the rows from `descriptions` 
           whose `id` exists in the daily ads subset.

    Args:
        descriptions (pd.DataFrame): Raw descriptions dataset containing ad IDs and text fields.
        ads (pd.DataFrame): Subset of ads for the currently processed date.

    Returns:
        pd.DataFrame: Cleaned and filtered descriptions matching the given daily ads.
    """

from .prepare_daily_ads import replace_empty_with_na

def prepare_daily_descriptions(descriptions, ads):
    descriptions = replace_empty_with_na(descriptions)
    daily_ids = set(ads["id"])

    return descriptions[descriptions["id"].isin(daily_ids)]