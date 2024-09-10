import pandas as pd
import polars as pl
from sklearn.model_selection import KFold
from xgboost import XGBClassifier


def binary_classifier(
    dat: pd.DataFrame,
    meta: pd.DataFrame,
    n_splits: int,
    shuffle: bool = False,
) -> pl.DataFrame:
    X = dat.drop(columns=["Label"])
    y = dat["Label"]

    if shuffle:
        y = y.sample(frac=1, random_state=42).reset_index(drop=True)

    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)

    pred_df = []
    fold = 1
    for train_index, val_index in kf.split(X):
        X_fold_train, X_fold_val = X.iloc[train_index], X.iloc[val_index]
        y_fold_train, y_fold_val = y.iloc[train_index], y.iloc[val_index]
        meta_fold_val = meta.iloc[val_index]

        # Initialize the model
        model = XGBClassifier(
            objective="binary:logistic",
            n_estimators=150,
            tree_method="hist",
            learning_rate=0.05,
            scale_pos_weight=(y_fold_train == 0).sum() / (y_fold_train == 1).sum(),
        )

        # Train the model on the fold training set
        model.fit(X_fold_train, y_fold_train)

        # Validate the model on the fold validation set
        y_fold_prob = model.predict_proba(X_fold_val)[:, 1]
        y_fold_pred = model.predict(X_fold_val)

        pred_df.append(
            pl.DataFrame({
                "Metadata_Compound": list(meta_fold_val["Metadata_Compound"]),
                "y_prob": list(y_fold_prob),
                "y_pred": list(y_fold_pred),
                "y_actual": list(y_fold_val),
                "k_fold": fold,
            })
        )
        fold = fold + 1

    return pl.concat(pred_df, how="vertical")


def classify_DILI_binary(input_path: str, label_column: str, output_path: str) -> None:
    dat = pl.read_parquet(input_path)

    agg_types = dat.select("Metadata_AggType").to_series().unique().to_list()
    pred_df = []
    for agg_type in agg_types:
        prof = dat.filter(
            (pl.col("Metadata_AggType") == agg_type)
            & (pl.col(label_column).is_not_null()).rename({label_column: "Label"})
        )
        meta_cols = [i for i in prof.columns if "Metadata_" in i]

        prof_meta = prof.select(meta_cols)
        prof = prof.drop(meta_cols)

        class_res = binary_classifier(
            prof.to_pandas(), prof_meta.to_pandas(), n_splits=10, shuffle=False
        )
        class_res = class_res.with_columns(
            pl.lit(agg_type).alias("Metadata_AggType"),
            pl.lit("Actual labels").alias("Metadata_LabelType"),
        )
        pred_df.append(class_res)

        shuffle_res = binary_classifier(
            prof.to_pandas(), prof_meta.to_pandas(), n_splits=10, shuffle=True
        )
        shuffle_res = shuffle_res.with_columns(
            pl.lit(agg_type).alias("Metadata_AggType"),
            pl.lit("Shuffled labels").alias("Metadata_LabelType"),
        )
        pred_df.append(shuffle_res)

    pred_df = pl.concat(pred_df, how="vertical")
    pred_df.write_parquet(output_path)
