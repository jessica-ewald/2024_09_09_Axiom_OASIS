import numpy as np
import pandas as pd
import polars as pl
import xgboost as xgb
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.model_selection import GroupShuffleSplit


def xgboost_regression(dat: pd.DataFrame, target: str, feat_cols: list, split_group: str, *, mean_pred: bool = False) -> pl.DataFrame:
    """Predict continuous metadata from profiling data with XGBoost."""

    groups = dat[split_group]
    gss = GroupShuffleSplit(n_splits=5, test_size=0.2, random_state=42)
    i = 1
    results = []
    pred_obs = []

    for split_idx, (train_idx, test_idx) in tqdm(enumerate(gss.split(dat, groups=groups)), desc=f"Processing {target}"):
        # Splitting the dataset
        train_data = dat.iloc[train_idx]
        test_data = dat.iloc[test_idx]

        train_data = train_data.dropna(subset=[target]).reset_index(drop=True)
        test_data = test_data.dropna(subset=[target]).reset_index(drop=True)
        
        X_train = train_data[feat_cols]
        y_train = train_data[target]
        X_test = test_data[feat_cols]
        y_test = test_data[target]

        # Train model
        if mean_pred:
            mean_value = np.mean(y_train)
            predictions = np.full(len(y_test), mean_value)
        else: 
            model = xgb.XGBRegressor(objective="reg:squarederror")
            model.fit(X_train, y_train)
            predictions = model.predict(X_test)

        # Calculate performance
        mse = mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_test, predictions)

        results.append((target, split_idx, r2, rmse, mae))
        pred_obs.append(pl.DataFrame({
            "Predicted": predictions,
            "Observed": y_test.values,
            "Metadata_Plate": test_data["Metadata_Plate"].values,
            "Metadata_Well": test_data["Metadata_Well"].values,
            "Metadata_Compound": test_data["Metadata_Compound"].values,
            "Metadata_OASIS_ID": test_data["Metadata_OASIS_ID"].values,
            "Metadata_Log10Conc": test_data["Metadata_Log10Conc"].values,
            "Variable": var,
            "Split": i,
        }))

    # Convert results to DataFrame for analysis
    results_df = pd.DataFrame(results, columns=["Variable", "Split", "R²", "RMSE", "MAE"])
    pred_obs_df = pl.concat(pred_obs, how="vertical")

    return results_df, pred_obs_df


def predict_axiom_assays(prof_path: str, prediction_path: str, results_path: str) -> None:
    """Train XGBoost regression model to predict Axiom assays."""
    profiles = pl.read_parquet(prof_path)
    feat_cols = [i for i in profiles.columns if "Metadata" not in i]
    baseline_cols = ["Metadata_Plate", "Metadata_Well", "Metadata_source", "Metadata_Count_Cells"]

    # LDH
    ldh_res, ldh_pred = xgboost_regression(
        profiles,
        "Metadata_ldh_ridge_norm",
        feat_cols,
        "Metadata_Compound"
    )
    ldh_pred = ldh_pred.with_columns(
        pl.lit("Metadata_ldh_ridge_norm").alias("Variable_Name"),
        pl.lit("Morphology").alias("Model_type"),
    )
    ldh_res = ldh_res.with_columns(
        pl.lit("Metadata_ldh_ridge_norm").alias("Variable_Name"),
        pl.lit("Morphology").alias("Model_type"),
    )

    ldh_bl_res, ldh_bl_pred = xgboost_regression(
        profiles,
        "Metadata_ldh_ridge_norm",
        baseline_cols,
        "Metadata_Compound"
    )
    ldh_bl_pred = ldh_bl_pred.with_columns(
        pl.lit("Metadata_ldh_ridge_norm").alias("Variable_Name"),
        pl.lit("Baseline").alias("Model_type"),
    )
    ldh_bl_res = ldh_bl_res.with_columns(
        pl.lit("Metadata_ldh_ridge_norm").alias("Variable_Name"),
        pl.lit("Baseline").alias("Model_type"),
    )

    ldh_mean_res, ldh_mean_pred = xgboost_regression(
        profiles,
        "Metadata_ldh_ridge_norm",
        [],
        "Metadata_Compound",
        mean_pred=True
    )
    ldh_mean_pred = ldh_mean_pred.with_columns(
        pl.lit("Metadata_ldh_ridge_norm").alias("Variable_Name"),
        pl.lit("Mean_predictor").alias("Model_type"),
    )
    ldh_mean_res = ldh_mean_res.with_columns(
        pl.lit("Metadata_ldh_ridge_norm").alias("Variable_Name"),
        pl.lit("Mean_predictor").alias("Model_type"),
    )

    # MTT
    mtt_res, mtt_pred = xgboost_regression(
        profiles,
        "Metadata_mtt_ridge_norm",
        feat_cols,
        "Metadata_Compound"
    )
    mtt_pred = mtt_pred.with_columns(
        pl.lit("Metadata_mtt_ridge_norm").alias("Variable_Name"),
        pl.lit("Morphology").alias("Model_type"),
    )
    mtt_res = mtt_res.with_columns(
        pl.lit("Metadata_mtt_ridge_norm").alias("Variable_Name"),
        pl.lit("Morphology").alias("Model_type"),
    )

    mtt_bl_res, mtt_bl_pred = xgboost_regression(
        profiles,
        "Metadata_mtt_ridge_norm",
        baseline_cols,
        "Metadata_Compound"
    )
    mtt_bl_pred = mtt_bl_pred.with_columns(
        pl.lit("Metadata_mtt_ridge_norm").alias("Variable_Name"),
        pl.lit("Baseline").alias("Model_type"),
    )
    mtt_bl_res = mtt_bl_res.with_columns(
        pl.lit("Metadata_mtt_ridge_norm").alias("Variable_Name"),
        pl.lit("Baseline").alias("Model_type"),
    )

    mtt_mean_res, mtt_mean_pred = xgboost_regression(
        profiles,
        "Metadata_mtt_ridge_norm",
        [],
        "Metadata_Compound",
        mean_pred=True
    )
    mtt_mean_pred = mtt_mean_pred.with_columns(
        pl.lit("Metadata_mtt_ridge_norm").alias("Variable_Name"),
        pl.lit("Mean_predictor").alias("Model_type"),
    )
    mtt_mean_res = mtt_mean_res.with_columns(
        pl.lit("Metadata_mtt_ridge_norm").alias("Variable_Name"),
        pl.lit("Mean_predictor").alias("Model_type"),
    )

    # Write out predictions
    prediction_df = pl.concat(
        [ldh_pred, ldh_bl_pred, ldh_mean_pred, mtt_pred, mtt_bl_pred, mtt_mean_pred], 
        how="vertical_relaxed"
    ).with_columns(
        pl.col("Observed").arr.first().cast(pl.Float32).alias("Observed")
    ).write_parquet(prediction_path)

    # Write out results
    res_df = pl.concat(
        [ldh_res, ldh_bl_res, ldh_mean_res, mtt_res, mtt_bl_res, mtt_mean_res], 
        how="vertical_relaxed"
    ).write_parquet(results_path)