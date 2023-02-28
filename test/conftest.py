import pytest
import logging
import requests
from pydantic import BaseModel
import os
import json
import yaml
import base_utils as bu
import re


# path to test directory
pytest.test_dir = os.path.dirname(os.path.abspath(__file__))

# load base path directly from config file
base_path_league_exp_v4 = bu.read_yaml(
    f"{os.path.dirname(pytest.test_dir)}"
    f"/Lol_job/resources/other_config/dev_config.yaml"
)["base_links"]["step_1_league_exp_v4"]

# base path for get_summoners test
pytest.base_path_step_1 = "base_path_get_summoner_test"

# base path for get_puuid test
pytest.base_path_step_2 = "base_path_get_puuid_test"

# base path for step 3_4 test
pytest.base_path_step_3_4 = "base_path_get_games"

# base path for step 5 test
pytest.base_path_step_5 = "get_games_details_base_path"

# base path and constant for step 6 test
pytest.base_path_step_6 = "get_missing_games_list_path"
pytest.game_type = ""

# base path for step 7 test
pytest.base_path_step_7_games = "step_7_get_games_info"
pytest.base_path_step_7_puuid = "step_7_get_puuid_info"
pytest.base_path_step_7_encrypted_id = "step_7_get_encrypted_id_info"

# base path for step 8 test
pytest.base_path_step_8 = "step_8_get_champ_exp"

# base path for step 8 test
pytest.base_path_step_9_version = "step_9_get_version"
pytest.base_path_step_9_champs = "step_9_get_champs/{}"


# initial structure for step_1 tests
pytest.initial_structure = {
    "queue_type": "RANKED_SOLO_5x5",
    "tiers": ["CHALLENGER", "GRANDMASTER", "MASTER"],
    "divisions": ["I"],
}

# basic objects to be shared through the tests
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
pytest.logger = logging.getLogger(__name__)

pytest.header = {}
pytest.verbose = False


# loading content for each tier in tests for step 1
with open(f"{pytest.test_dir}/master_content", mode="rb") as file:
    pytest.master_content = file.read()
with open(f"{pytest.test_dir}/grandmaster_content", mode="rb") as file:
    pytest.grandmaster_content = file.read()
with open(f"{pytest.test_dir}/challenger_content", mode="rb") as file:
    pytest.challenger_content = file.read()


# load content for get_summoners step 1
with open(f"{pytest.test_dir}/get_summoners_answer", mode="rb") as file:
    pytest.get_summoners_answer = file.read()

# load content for get_puuid step 2
with open(f"{pytest.test_dir}/get_puuid_answer.json", mode="rb") as file:
    pytest.get_puuid_answer = json.load(file)

# load content for answers step 3_4
with open(f"{pytest.test_dir}/step_3_answer.json", mode="rb") as file:
    pytest.get_games_answer = json.load(file)

# load content for answers step 5
with open(f"{pytest.test_dir}/step_5_answer.json", mode="rb") as file:
    pytest.step_5_answer = json.load(file)

# load content for answers step 6
with open(f"{pytest.test_dir}/step_6_answer.yaml", mode="rb") as file:
    pytest.step_6_answer = yaml.load(file, Loader=yaml.FullLoader)

# load content for answers step 7
with open(f"{pytest.test_dir}/step_7_answers_games_details.json", mode="rb") as file:
    pytest.step_7_answer_games = json.load(file)
with open(
    f"{pytest.test_dir}/step_7_answers_players_encrypted_id.yaml", mode="rb"
) as file:
    pytest.step_7_answer_encrypted_id = yaml.load(file, Loader=yaml.FullLoader)
with open(f"{pytest.test_dir}/step_7_answers_players_info.yaml", mode="rb") as file:
    pytest.step_7_answer_players_info = yaml.load(file, Loader=yaml.FullLoader)

# load content for answers step 8
with open(f"{pytest.test_dir}/step_8_answer.yaml", mode="rb") as file:
    pytest.step_8_answer = yaml.load(file, Loader=yaml.FullLoader)

# load content for answers step 9
with open(f"{pytest.test_dir}/step_9_answers.yaml", "r") as fp:
    pytest.step_9_answer = yaml.load(fp, Loader=yaml.FullLoader)


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
        elif req.startswith(pytest.base_path_step_1):
            return Answer(
                **{"status_code": 200, "content": pytest.get_summoners_answer}
            )
        elif req.startswith(pytest.base_path_step_2):

            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.get_puuid_answer[req.split("/")[-1]]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_3_4):
            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.get_games_answer[req.split("/")[-2]]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_5):

            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_5_answer[req.split("/")[-1]]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_6):
            match = re.search(r"start=(\d+)&count=(\d+)", req)
            id_start = int(match.group(1))
            how_many = int(match.group(2))
            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_6_answer[req.split("/")[-2]][
                            id_start : id_start + how_many
                        ]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_7_games):

            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_7_answer_games[req.split("/")[-1]]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_7_puuid):
            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_7_answer_encrypted_id[req.split("/")[-1]]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_7_encrypted_id):

            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_7_answer_players_info[req.split("/")[-1]]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_8):
            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_8_answer[req.split("/")[-3]][
                            f"lvl_{req.split('/')[-1]}"
                        ]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_9_version):
            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_9_answer["versions_input"]
                    ).encode(),
                }
            )
        elif req.startswith(pytest.base_path_step_9_champs.split("/")[-2]):
            return Answer(
                **{
                    "status_code": 200,
                    "content": json.dumps(
                        pytest.step_9_answer["champs_input"]
                    ).encode(),
                }
            )

    monkeysession.setattr(requests, "get", mock_get)
