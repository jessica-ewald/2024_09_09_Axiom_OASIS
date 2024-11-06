import matplotlib as mpl
import numpy as np
import pandas as pd
import plotnine as pn
import polars as pl
import xgboost as xgb
from matplotlib.backends.backend_pdf import PdfPages
from plotnine import aes, geom_line, geom_point, ggplot, labs, theme_bw
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold


def xgboost_regression(dat: pl.DataFrame, target: str, feat_cols: list, id_cols: list, *, shuffle: bool = False) -> pl.DataFrame:
    """Predict continuous metadata from profiling data with XGBoost."""
    x = dat.select(feat_cols).to_numpy()

    if shuffle:
        y = dat.select(pl.col(target).shuffle(seed=42)).to_numpy()
    else:
        y = dat.select(pl.col(target)).to_numpy()

    model = xgb.XGBRegressor(objective="reg:squarederror")
    kf = KFold(n_splits=5, shuffle=True, random_state=42)

    observed_values = []
    predicted_values = []
    id_values = []
    for train_index, test_index in kf.split(x):
        # Split the data into training and testing sets
        x_train, x_test = x[train_index], x[test_index]
        y_train, y_test = y[train_index], y[test_index]

        # Train the model
        model.fit(x_train, y_train)

        # Predict on the test set
        y_pred = model.predict(x_test)
        observed_values.extend(y_test)
        predicted_values.extend(y_pred)
        id_values.extend(dat.select(id_cols).to_numpy()[test_index])

    result_df = pl.DataFrame({
        "Observed": observed_values,
        "Predicted": predicted_values,
    })

    id_df = pl.DataFrame(id_values, schema={col: dat[col].dtype for col in id_cols})
    return pl.concat([id_df, result_df], how="horizontal")

def make_plots(pred: pl.DataFrame, output_path: str) -> None:
    """Plot observed vs. predicted and mse per bin."""

    # TODO add R2 to overall plot
    mpl.use("Agg")
    with PdfPages(output_path) as pdf:
        pn.options.figure_size = (8, 8)

        plot1 = (
            ggplot(pred.filter(pl.col("Variable_Name") == "Metadata_ldh_normalized"), aes(x="Observed", y="Predicted"))
            + geom_point(alpha=0.6)
            + labs(
                x="Observed",
                y="Predicted",
                title="Observed vs Predicted LDH",
            )
            + theme_bw()
        )
        pdf.savefig(plot1.draw())

        plot2 = (
            ggplot(pred.filter(pl.col("Variable_Name") == "Metadata_mtt_normalized"), aes(x="Observed", y="Predicted"))
            + geom_point(alpha=0.6)
            + labs(
                x="Observed",
                y="Predicted",
                title="Observed vs Predicted MTT",
            )
            + theme_bw()
        )
        pdf.savefig(plot2.draw())

        plot3 = (
            ggplot(pred.filter(pl.col("Variable_Name") == "Metadata_Count_Cells"), aes(x="Observed", y="Predicted"))
            + geom_point(alpha=0.6)
            + labs(
                x="Observed",
                y="Predicted",
                title="Observed vs Predicted cell count",
            )
            + theme_bw()
        )
        pdf.savefig(plot3.draw())


def predict_axiom_assays(prof_path: str, prediction_path: str, plot_path: str, *, shuffle: bool = False) -> None:
    """Train XGBoost regression model to predict Axiom assays."""
    profiles = pl.read_parquet(prof_path)
    feat_cols = [i for i in profiles.columns if "Metadata" not in i]
    id_cols = ["Metadata_Plate", "Metadata_Well", "Metadata_Compound", "Metadata_Log10Conc"]

    ldh = xgboost_regression(
        profiles.filter(pl.col("Metadata_ldh_normalized") > -0.5),
        "Metadata_ldh_normalized",
        feat_cols,
        id_cols,
        shuffle=shuffle
    )
    ldh = ldh.with_columns(
        pl.lit("Metadata_ldh_normalized").alias("Variable_Name"),
    )

    mtt = xgboost_regression(profiles, "Metadata_mtt_normalized", feat_cols, id_cols, shuffle=shuffle)
    mtt = mtt.with_columns(
        pl.lit("Metadata_mtt_normalized").alias("Variable_Name"),
    )

    cc = xgboost_regression(profiles, "Metadata_Count_Cells", feat_cols, id_cols, shuffle=shuffle)
    cc = cc.with_columns(
        pl.lit("Metadata_Count_Cells").alias("Variable_Name"),
    )

    # Write out predictions
    prediction_df = pl.concat([ldh, mtt, cc], how="vertical_relaxed")
    prediction_df = prediction_df.with_columns(
        pl.col("Observed").arr.first().cast(pl.Float32).alias("Observed")
    )
    prediction_df.write_parquet(prediction_path)

    # Analyze results
    make_plots(prediction_df, plot_path)