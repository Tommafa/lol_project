import pytest
import logging
import requests
from pydantic import BaseModel
import os
import resources.base_utils as bu

# path to test directory
pytest.test_dir = os.path.dirname(os.path.abspath(__file__))

# load base path directly from config file
base_path_league_exp_v4 = bu.read_yaml(
    f"{os.path.dirname(pytest.test_dir)}" f"/resources/other_config/dev_config.yaml"
)["base_links"]["league_exp_v4"]

# base path for get_summoners test
pytest.get_summoners_base_path = "base_path_get_summoner_test"


# initial structure for step_1 tests
pytest.initial_structure = {
    "queue_type": "RANKED_SOLO_5x5",
    "tiers": ["CHALLENGER", "GRANDMASTER", "MASTER"],
    "divisions": ["I"],
}

# basic objects to be shared through the tests
pytest.logger = logging.getLogger()
pytest.header = {}
pytest.verbose = True


# loading content for each tier in tests
with open(f"{pytest.test_dir}/master_content", mode="rb") as file:
    pytest.master_content = file.read()
with open(f"{pytest.test_dir}/grandmaster_content", mode="rb") as file:
    pytest.grandmaster_content = file.read()
with open(f"{pytest.test_dir}/challenger_content", mode="rb") as file:
    pytest.challenger_content = file.read()

with open(f"{pytest.test_dir}/get_summoners_answer", mode="rb") as file:
    pytest.get_summoners_answer = file.read()


# basic object for requests mocked returned object
class Answer(BaseModel):
    status_code: int = 200
    content: bytes


# fixture to allow mocking with session scope
@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


# mock of the requests.get function
@pytest.fixture(autouse=True, scope="session")
def mock_response(monkeysession):
    """Requests.get() mocked to return {'mock_key':'mock_response'}."""

    def mock_get(req, **kwargs):
        if req.startswith(f"{base_path_league_exp_v4}RANKED_SOLO_5x5/CHALLENGER/"):
            return Answer(**{"status_code": 200, "content": pytest.challenger_content})
        elif req.startswith(f"{base_path_league_exp_v4}RANKED_SOLO_5x5/GRANDMASTER/"):
            return Answer(**{"status_code": 200, "content": pytest.grandmaster_conten})
        elif req.startswith(f"{base_path_league_exp_v4}RANKED_SOLO_5x5/MASTER/"):
            return Answer(**{"status_code": 200, "content": pytest.master_content})
        elif req.startswith(pytest.get_summoners_base_path):
            return Answer(
                **{"status_code": 200, "content": pytest.get_summoners_answer}
            )

    monkeysession.setattr(requests, "get", mock_get)
