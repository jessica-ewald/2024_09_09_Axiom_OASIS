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


def xgboost_regression(dat: pl.DataFrame, target: str, feat_cols: list, id_cols: list) -> pl.DataFrame:
    """Predict continuous metadata from profiling data with XGBoost."""
    x = dat.select(feat_cols).to_numpy()
    y = dat[target].to_numpy()

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


def mse_per_bin(dat: pl.DataFrame, n_bins: int, variable: str) -> (pl.DataFrame, pl.DataFrame):
    """Compute the mean squred error per observation bin for an XGBoost regression model."""
    dat = dat.filter(pl.col("Variable_Name") == variable)
    observed_values = dat["Observed"].to_numpy()
    bins = np.linspace(observed_values.min(), observed_values.max(), n_bins + 1)
    bin_indices = np.digitize(observed_values, bins) - 1

    dat = dat.with_columns([
        pl.Series(name="Observed_bins", values=bin_indices),
    ])

    mse_per_bin = dat.group_by("Observed_bins").agg([
        pl.map_groups(
            [pl.col("Observed"), pl.col("Predicted")],
            lambda x: mean_squared_error(x[0], x[1]),
        ).alias("mse"),
    ])

    dat = dat.to_pandas()
    mse_per_bin = mse_per_bin.to_pandas()

    bin_midpoints = []
    mse_values = []
    for i in range(len(bins) - 1):
        bin_mid = (bins[i] + bins[i + 1]) / 2
        if i in mse_per_bin["Observed_bins"]:
            bin_midpoints.append(bin_mid)
            mse_values.append(mse_per_bin["mse"][i])

    mse_df = pd.DataFrame({
        "bin_mid": bin_midpoints,
        "mse": mse_values,
    })

    return dat, mse_df


def make_plots(pred: pl.DataFrame, output_path: str) -> None:
    """Plot observed vs. predicted and mse per bin."""
    ldh, mse_ldh = mse_per_bin(pred, 10, "Metadata_ldh_normalized")
    mtt, mse_mtt = mse_per_bin(pred, 10, "Metadata_mtt_normalized")
    cc, mse_cc = mse_per_bin(pred, 10, "Metadata_Count_Cells")

    # TODO add R2 to overall plot
    mpl.use("Agg")
    with PdfPages(output_path) as pdf:
        pn.options.figure_size = (8, 8)

        plot1 = (
            ggplot(ldh, aes(x="Observed", y="Predicted"))
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
            ggplot(mse_ldh, aes(x="bin_mid", y="mse"))
            + geom_line()
            + theme_bw()
            + labs(
                x="Observed LDH (bin)",
                y="MSE",
            )
        )
        pdf.savefig(plot2.draw())

        plot3 = (
            ggplot(mtt, aes(x="Observed", y="Predicted"))
            + geom_point(alpha=0.6)
            + labs(
                x="Observed",
                y="Predicted",
                title="Observed vs Predicted MTT",
            )
            + theme_bw()
        )
        pdf.savefig(plot3.draw())

        plot4 = (
            ggplot(mse_mtt, aes(x="bin_mid", y="mse"))
            + geom_line()
            + theme_bw()
            + labs(
                x="Observed MTT (bin)",
                y="MSE",
            )
        )
        pdf.savefig(plot4.draw())

        plot5 = (
            ggplot(cc, aes(x="Observed", y="Predicted"))
            + geom_point(alpha=0.6)
            + labs(
                x="Observed",
                y="Predicted",
                title="Observed vs Predicted cell count",
            )
            + theme_bw()
        )
        pdf.savefig(plot5.draw())

        plot6 = (
            ggplot(mse_cc, aes(x="bin_mid", y="mse"))
            + geom_line()
            + theme_bw()
            + labs(
                x="Observed cell count (bin)",
                y="MSE",
            )
        )
        pdf.savefig(plot6.draw())


def predict_continuous(prof_path: str, prediction_path: str, plot_path: str) -> None:
    """Train XGBoost regression models and plot results."""
    profiles = pl.read_parquet(prof_path)
    profiles = profiles.with_columns(
        (pl.col("Metadata_ldh_normalized") / pl.col("Metadata_Count_Cells")).alias("Metadata_ldh_cc"),
        (pl.col("Metadata_mtt_normalized") / pl.col("Metadata_Count_Cells")).alias("Metadata_mtt_cc"),
    )
    feat_cols = [i for i in profiles.columns if "Metadata" not in i]
    id_cols = ["Metadata_Plate", "Metadata_Well", "Metadata_Compound", "Metadata_Log10Conc"]

    ldh = xgboost_regression(
        profiles.filter(pl.col("Metadata_ldh_normalized") > -0.5),
        "Metadata_ldh_normalized",
        feat_cols,
        id_cols,
    )
    ldh = ldh.with_columns(
        pl.lit("Metadata_ldh_normalized").alias("Variable_Name"),
    )

    mtt = xgboost_regression(profiles, "Metadata_mtt_normalized", feat_cols, id_cols)
    mtt = mtt.with_columns(
        pl.lit("Metadata_mtt_normalized").alias("Variable_Name"),
    )

    cc = xgboost_regression(profiles, "Metadata_Count_Cells", feat_cols, id_cols)
    cc = cc.with_columns(
        pl.lit("Metadata_Count_Cells").alias("Variable_Name"),
    )
    cc_cols = cc.columns
    cc = cc.with_columns(
        pl.col("Observed").cast(pl.Float32).alias("Observed"),
        pl.col("Predicted").cast(pl.Float32).alias("Predicted"),
    ).select(cc_cols)

    # Write out predictions
    prediction_df = pl.concat([ldh, mtt, cc], how="vertical_relaxed")
    prediction_df.write_parquet(prediction_path)

    # Analyze results
    make_plots(prediction_df, plot_path)
