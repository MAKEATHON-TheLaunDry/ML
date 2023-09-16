from typing import Tuple, Union, List
import numpy as np
from sklearn.linear_model import LogisticRegression
import openml
import pandas as pd
import glob
from aioarango import ArangoClient


#  train test split sklearn import
from sklearn.model_selection import train_test_split

XY = Tuple[np.ndarray, np.ndarray]
Dataset = Tuple[XY, XY]
LogRegParams = Union[XY, Tuple[np.ndarray]]
XYList = List[XY]


def get_model_parameters(model: LogisticRegression) -> LogRegParams:
    """Returns the paramters of a sklearn LogisticRegression model."""
    if model.fit_intercept:
        params = [
            model.coef_,
            model.intercept_,
        ]
    else:
        params = [
            model.coef_,
        ]
    return params


def set_model_params(
    model: LogisticRegression, params: LogRegParams
) -> LogisticRegression:
    """Sets the parameters of a sklean LogisticRegression model."""
    model.coef_ = params[0]
    if model.fit_intercept:
        model.intercept_ = params[1]
    return model


def set_initial_params(model: LogisticRegression, n_classes: int, n_features: int):
    """Sets initial parameters as zeros Required since model params are uninitialized
    until model.fit is called.

    But server asks for initial parameters from clients at launch. Refer to
    sklearn.linear_model.LogisticRegression documentation for more information.
    """
    # get number of classes and features of dataset 
    n_classes = n_classes  # MNIST has 10 classes
    n_features = n_features # Number of features in dataset
    model.classes_ = np.array([i for i in range(10)])

    model.coef_ = np.zeros((n_classes, n_features))
    if model.fit_intercept:
        model.intercept_ = np.zeros((n_classes,))


def load_bankingdata():
    # Specify the pattern to match CSV files (e.g., data_*.csv)

    #Concatenate all DataFrames into a single DataFrame
    data = connecto_to_arango()
    data = pd.DataFrame(data)
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.8, test_size=0.2, random_state=42, stratify=data['Is Laundering'])
    return (X_train, y_train), (X_test, y_test)

async def connecto_to_arango():
    client = ArangoClient(hosts="http://localhost:8529")

    # Connect to "_system" database as root user.
    db = await client.db("Transactions", username=str("root"), password=str("Blogchain"))
    cursor =  db.aql.execute('FOR p IN transactions RETURN p', count=True)
    doc = [doc for doc in cursor][0]
    data = pd.DataFrame(doc)
    X = data.drop(['Is Laundering'], axis=1)
    y = data['Is Laundering']
    return data


def shuffle(X: np.ndarray, y: np.ndarray) -> XY:
    """Shuffle X and y."""
    rng = np.random.default_rng()
    idx = rng.permutation(len(X))
    return X[idx], y[idx]


def partition(X: np.ndarray, y: np.ndarray, num_partitions: int) -> XYList:
    """Split X and y into a number of partitions."""
    return list(
        zip(np.array_split(X, num_partitions), np.array_split(y, num_partitions))
    )
