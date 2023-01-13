import pytest
import Lol_job.Lol_job.resources.riot_api_readers as riot_r
from Lol_job.Lol_job.resources.base_utils import make_request
import json
import pickle


@pytest.mark.step_1
def test_build_summoners_links_per_division():
    """given the structure for challenger, grandmaster and master,
    it produces the list of json as expected in the #
    given section"""
    # GIVEN

    with open(
        f"{pytest.test_dir}/final_structure_build_summoners_links_per_division", "rb"
    ) as fp:
        expected_final_structure = pickle.load(fp)

    base_path = "base_path"

    # WHEN
    computed_final_structure = riot_r.build_summoners_links_per_division(
        base_path, pytest.initial_structure, pytest.header
    )

    # THEN

    assert computed_final_structure == expected_final_structure


@pytest.mark.step_1
def test_make_request():
    """this test makes a request to mocked Request for the master
    players and returns the players as a list of LeagueEntryDTO"""
    # GIVEN
    request_string = (
        "https://euw1.api.riotgames.com/lol/league-exp"
        "/v4/entries/RANKED_SOLO_5x5/MASTER/I?page=1"
    )

    # WHEN
    result = make_request(
        request_string, pytest.header, verbose=pytest.verbose, logger=pytest.logger
    )
    # THEN
    assert result == pytest.master_content


@pytest.mark.step_1
def test_get_summoners():
    """this test"""
    # GIVEN
    base_path = pytest.get_summoners_base_path
    pages = 1
    with open(f"{pytest.test_dir}/get_summoners_expected_answer.json", "r") as fp:
        expected_summoners_dict = json.load(fp)
    with open(
        f"{pytest.test_dir}/get_mini_series_dict_expected_answer.json", "r"
    ) as fp:
        expected_mini_series_dict = json.load(fp)

    # WHEN
    summoners_dict, mini_series_dict = riot_r.get_summoners(
        pytest.logger,
        base_path,
        pytest.initial_structure,
        pytest.header,
        pages=pages,
        verbose=pytest.verbose,
    )
    # THEN
    assert summoners_dict == expected_summoners_dict
    assert mini_series_dict == expected_mini_series_dict


@pytest.mark.step_2
def test_load_puuid_for_summoner():
    # GIVEN
    base_path = pytest.get_summoners_base_path
    pages = 1
    with open(f"{pytest.test_dir}/get_summoners_expected_answer.json", "r") as fp:
        expected_summoners_dict = json.load(fp)
    with open(
        f"{pytest.test_dir}/get_mini_series_dict_expected_answer.json", "r"
    ) as fp:
        expected_mini_series_dict = json.load(fp)

    # WHEN
    summoners_dict, mini_series_dict = riot_r.get_summoners(
        pytest.logger,
        base_path,
        pytest.initial_structure,
        pytest.header,
        pages=pages,
        verbose=pytest.verbose,
    )
    # THEN
    assert summoners_dict == expected_summoners_dict
    assert mini_series_dict == expected_mini_series_dict
