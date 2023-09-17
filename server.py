import flwr as fl
import utils
from sklearn.metrics import log_loss, precision_score
from sklearn.linear_model import LogisticRegression
from typing import Dict
import os
import numpy as np


def fit_round(server_round: int) -> Dict:
    """Send round number to client."""
    return {"server_round": server_round}


def get_evaluate_fn(model: LogisticRegression):
    """Return an evaluation function for server-side evaluation."""

    # Load test data here to avoid the overhead of doing it in `evaluate` itself
    _, (X_test, y_test) = utils.load_bankingdata()
    print(X_test.shape, y_test.shape)
    print(model.predict(X_test).shape)
    # print types
    print(type(X_test), type(y_test.values))
    print(type(model.predict(X_test)))
    # print unique values
    print(np.unique(y_test.values))
    print(np.unique(model.predict(X_test)))
    # print value types
    print(y_test.values.dtype, model.predict(X_test).dtype)

    

    # The `evaluate` function will be called after every round
    def evaluate(server_round, parameters: fl.common.NDArrays, config):
        # Update model with the latest parameters
        utils.set_model_params(model, parameters)
        loss = log_loss(y_test, model.predict_proba(X_test))
        accuracy = model.score(X_test, y_test)
        precision = precision_score(y_test.values, model.predict(X_test))
        return loss, {"accuracy": accuracy, "precision": precision}

    return evaluate


# Start Flower server for five rounds of federated learning
if __name__ == "__main__":
    model = LogisticRegression()
    utils.set_initial_params(model, 2, 44)
    strategy = fl.server.strategy.FedAvg(
        min_available_clients=int(os.environ.get("MIN_CLIENTS", 2)),
        evaluate_fn=get_evaluate_fn(model),
        on_fit_config_fn=fit_round,
    )
    fl.server.start_server(
        server_address="0.0.0.0:8080",
        strategy=strategy,
        config=fl.server.ServerConfig(num_rounds=10),
    )
