import traceback  # noqa: CPY001, D100

import pandas as pd
import polars as pl
from sklearn.model_selection import StratifiedKFold
from tqdm import tqdm
from xgboost import XGBClassifier


def binary_classifier(
    dat: pd.DataFrame,
    meta: pd.DataFrame,
    n_splits: int,
    shuffle: bool = False,
) -> pl.DataFrame:
    dat["Label"] = dat["Label"].astype(int)
    x = dat.drop(columns=["Label"])
    y = dat["Label"]

    if shuffle:
        y = y.sample(frac=1, random_state=42).reset_index(drop=True)

    kf = StratifiedKFold(n_splits=n_splits)

    pred_df = []
    fold = 1
    for train_index, val_index in kf.split(x, y):
        x_fold_train, x_fold_val = x.iloc[train_index], x.iloc[val_index]
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
        model.fit(x_fold_train, y_fold_train)

        # Validate the model on the fold validation set
        y_fold_prob = model.predict_proba(x_fold_val)[:, 1]
        y_fold_pred = model.predict(x_fold_val)

        pred_df.append(
            pl.DataFrame({
                "Metadata_OASIS_ID": list(meta_fold_val["Metadata_OASIS_ID"]),
                "y_prob": list(y_fold_prob),
                "y_pred": list(y_fold_pred),
                "y_actual": list(y_fold_val),
            }),
        )
        fold += 1

    return pl.concat(pred_df, how="vertical")


def predict_seal_binary(input_path: str, label_path: str, output_path: str) -> None:
    n_splits = 5

    dat = pl.read_parquet(input_path)
    meta = pl.read_parquet(label_path).rename({"OASIS_ID": "Metadata_OASIS_ID"})
    labels = [i for i in meta.columns if "Metadata_" not in i]

    dat = dat.join(meta, on="Metadata_OASIS_ID", how="left")

    agg_types = dat.select("Metadata_AggType").to_series().unique().to_list()
    pred_df = []

    for label_column in tqdm(labels):
        for agg_type in agg_types:
            prof = dat.filter(
                (pl.col("Metadata_AggType") == agg_type) & (pl.col(label_column).is_not_null()),
            ).rename({label_column: "Label"})

            num_0 = prof.filter(pl.col("Label") == 0).height
            num_1 = prof.filter(pl.col("Label") == 1).height

            if (num_0 >= n_splits) & (num_1 >= n_splits):
                try:
                    meta_cols = [i for i in prof.columns if "Metadata_" in i]
                    all_meta_cols = [i for i in prof.columns if i in labels] + meta_cols

                    prof_meta = prof.select(meta_cols)
                    prof = prof.drop(all_meta_cols)

                    class_res = binary_classifier(
                        prof.to_pandas(),
                        prof_meta.to_pandas(),
                        n_splits=n_splits,
                        shuffle=False,
                    )
                    class_res = class_res.with_columns(
                        pl.lit(agg_type).alias("Metadata_AggType"),
                        pl.lit(label_column).alias("Metadata_Label"),
                        pl.lit(num_0).alias("Metadata_Count_0"),
                        pl.lit(num_1).alias("Metadata_Count_1"),
                    )
                    pred_df.append(class_res)
                except Exception:  # noqa: BLE001
                    print(f"An error occurred for label '{label_column}' and aggregation type '{agg_type}':")
                    print(traceback.format_exc())

    pred_df = pl.concat(pred_df, how="vertical")
    pred_df.write_parquet(output_path)
