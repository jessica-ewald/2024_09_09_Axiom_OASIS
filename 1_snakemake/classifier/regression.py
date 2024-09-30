import numpy as np
import polars as pl
import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold


def predict_continuous(profile_path: str, result_path: str) -> None:
    """XGBoost model for predicting continuous values."""
    dat = pl.read_parquet(profile_path)

    feat_cols = [i for i in dat.columns if "Metadata_" not in i]

    X = dat.select(feat_cols).to_numpy()
    y = dat["Metadata_mtt"].to_numpy()

    model = xgb.XGBRegressor()
    kf = KFold(n_splits=5, shuffle=True, random_state=42)

    fold = 1
    mse_scores = []
    for train_index, test_index in kf.split(X):
        # Split the data into training and testing sets
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

        # Train the model
        model.fit(X_train, y_train)

        # Predict on the test set
        y_pred = model.predict(X_test)

        # Calculate mean squared error for the current fold
        mse = mean_squared_error(y_test, y_pred)
        mse_scores.append(mse)

        print(f"Fold {fold}, MSE: {mse}")
        fold += 1

    print(f"Average MSE across 5 folds: {np.mean(mse_scores)}")
