import numpy as np
import pandas as pd

__QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95]


def update_quantile(
    upd: pd.DataFrame, col: str, val: float, f: pd.Series | None = None
):
    """
    Update a dataframe column with same value array
    :param upd: The dataframe to update
    :param col: The column to update
    :param val: the value to update
    :param f: optional dataframe filter
    """
    value = 0 if val == np.NaN else val
    if f is not None:
        upd.loc[f, col] = value
    else:
        upd[col] = np.full(len(upd), value)


if __name__ == "__main__":
    df = pd.read_csv("bq-results.csv")
    df["cost_per_night"] = df["gross_sales"] / df["package_nights"]
    df_country_offers = (
        df.groupby(["offer_country_code"])
        .offer_bk.nunique()
        .reset_index()
        .rename(columns={"offer_bk": "offers"})
    )
    # update quantiles with global values for countries with less than 10 offers
    df_low_quantiles = df_country_offers[df_country_offers.offers <= 10].reset_index(
        drop=True
    )
    for q in __QUANTILES:
        update_quantile(df_low_quantiles, str(q), df["cost_per_night"].quantile(q))

    df_high_quantiles = df_country_offers[df_country_offers.offers > 10].reset_index(
        drop=True
    )
    countries = df_high_quantiles.values.flatten().tolist()
    for country in countries:
        for q in __QUANTILES:
            filter_df = df_high_quantiles["offer_country_code"] == country
            quantile = df[df.offer_country_code == country]["cost_per_night"].quantile(
                q
            )
            update_quantile(df_high_quantiles, str(q), quantile, filter_df)
    df_country_quantiles = pd.concat(
        [df_low_quantiles, df_high_quantiles], ignore_index=True
    )
    print(df_country_quantiles)
