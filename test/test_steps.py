import pytest
import riot_api_readers as riot_r
from base_utils import make_request
import steps as st
import league_objects as lo
import json
import yaml
import pickle
import pandas as pd
from unittest import TestCase


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
    """this test makes a request to mocked Request to get the
    summoners for each rank in initial_structure"""
    # GIVEN
    base_path = pytest.base_path_step_1
    pages = 1
    with open(f"{pytest.test_dir}/step_1_summoners_expected_answer.json", "r") as fp:
        expected_summoners_dict = json.load(fp)
    with open(
        f"{pytest.test_dir}/step_1_mini_series_dict_expected_answer.json", "r"
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
    """this test retrieves for each summoner the puuid
    making a request to the mocked Requests package"""
    # GIVEN
    with open(f"{pytest.test_dir}/step_2_input_df.json", "r") as fp:
        input_df = pd.DataFrame.from_dict(json.load(fp))
    with open(f"{pytest.test_dir}/step_2_expected_answer.json", "r") as fp:
        expected_puuid_dict = pd.DataFrame.from_dict(json.load(fp))
    base_link = pytest.base_path_step_2
    column_of_interest = "summonerId"
    # WHEN
    puuids_df = riot_r.get_puuids(
        pytest.logger,
        base_link,
        pytest.header,
        pytest.verbose,
        input_df,
        column_of_interest,
    )
    # THEN
    pd.testing.assert_frame_equal(puuids_df, expected_puuid_dict)


@pytest.mark.step_3_4
def test_get_games_list_per_puuid():
    """this test checks that given a df with summoners,
    for each summoner a list of games' names is provided"""
    # GIVEN
    with open(f"{pytest.test_dir}/step_3_input_df.json", "r") as fp:
        input_df = pd.DataFrame.from_dict(json.load(fp))
    with open(f"{pytest.test_dir}/step_3_expected_outputs.json", "r") as fp:
        expected_get_games_dict = json.load(fp)

    games_to_load = 5
    historical_memory = 5
    puuid_column = "puuid"
    base_path = pytest.base_path_step_3_4
    # WHEN
    (potential_t0_games, players_of_a_game) = riot_r.get_games(
        logger=pytest.logger,
        base_link=base_path,
        header=pytest.header,
        verbose=pytest.verbose,
        starting_df=input_df,
        column_of_interest=puuid_column,
        number_of_games=games_to_load,
        historical_memory=historical_memory,
    )

    # THEN
    TestCase().assertDictEqual(
        expected_get_games_dict["players_of_a_game"], players_of_a_game
    )
    assert potential_t0_games == set(expected_get_games_dict["potential_t0_games"])


@pytest.mark.step_5
def test_step5():
    # GIVEN
    timestamp_job_day = 1674391814401
    with open(f"{pytest.test_dir}/step_5_inputs.yaml", "r") as fp:
        inputs = yaml.load(fp, Loader=yaml.FullLoader)
    with open(f"{pytest.test_dir}/step_5_expected_outputs.yaml", "r") as fp:
        outputs = yaml.load(fp, Loader=yaml.FullLoader)
    base_link = pytest.base_path_step_5
    # WHEN

    (
        output_potential_t0,
        output_list_of_missing_data,
        output_games_details_list,
        output_games_summoners_list,
    ) = st.step_5(
        logger=pytest.logger,
        header=pytest.header,
        base_link=base_link,
        players_of_a_game=inputs["players_of_a_game"],
        potential_t0_games=inputs["potential_t0_games"],
        current_time=timestamp_job_day,
        verbose=pytest.verbose,
    )

    # THEN
    assert output_games_details_list == outputs["Games_output"]
    assert output_games_summoners_list == outputs["Game_summoner_output"]
    assert output_potential_t0 == outputs["potential_t0_games"]
    assert output_list_of_missing_data == set(outputs["list_of_missing_data"])


@pytest.mark.step_6
def test_step6():
    # GIVEN
    with open(f"{pytest.test_dir}/step_6_inputs.yaml", "r") as fp:
        inputs = yaml.load(fp, Loader=yaml.FullLoader)
    with open(f"{pytest.test_dir}/step_6_expected_outputs.yaml", "r") as fp:
        outputs = yaml.load(fp, Loader=yaml.FullLoader)
    base_link = pytest.base_path_step_6
    historical_memory = 5
    # WHEN
    (summoners_to_retrieve, games_to_retrieve) = st.step_6(
        logger=pytest.logger,
        header=pytest.header,
        players_games_list_endpoint=base_link,
        list_of_missing_data=inputs["list_of_missing_data"],
        historical_memory=historical_memory,
        game_type=pytest.game_type,
        verbose=pytest.verbose,
        number_of_games=10,
    )

    # THEN
    assert summoners_to_retrieve == outputs["summoners_to_retrieve"]
    assert games_to_retrieve == outputs["games_to_retrieve"]


@pytest.mark.step_7
def test_step7():
    # GIVEN
    with open(f"{pytest.test_dir}/step_7_inputs.yaml", "r") as fp:
        inputs = yaml.load(fp, Loader=yaml.FullLoader)
    with open(f"{pytest.test_dir}/step_7_expected_outputs.yaml", "r") as fp:
        outputs = yaml.load(fp, Loader=yaml.FullLoader)
    base_link_games = pytest.base_path_step_7_games
    base_link_puuid = pytest.base_path_step_7_puuid
    base_link_encrypted_id = pytest.base_path_step_7_encrypted_id

    # WHEN
    (output_games_list, output_games_summoners_list, output_summoners_list) = st.step_7(
        logger=pytest.logger,
        header=pytest.header,
        base_link_games=base_link_games,
        base_link_puuid=base_link_puuid,
        base_link_encrypted_id=base_link_encrypted_id,
        games_to_retrieve=inputs["games_to_retrieve"],
        summoners_to_retrieve=inputs["summoners_to_retrieve"],
        verbose=pytest.verbose,
    )

    # THEN
    assert set(frozenset(d.items()) for d in output_games_list) == set(
        frozenset(d.items()) for d in outputs["games_output"]
    )
    assert set(frozenset(d.items()) for d in output_games_summoners_list) == set(
        frozenset(d.items()) for d in outputs["games_summoners_output"]
    )
    assert set(frozenset(d.items()) for d in output_summoners_list) == set(
        frozenset(lo.LeagueEntryDTO(**d).full_dict(d["puuid"]).items())
        for d in outputs["summoners_list_output"]
    )


@pytest.mark.step_8
def test_step8():
    # GIVEN
    with open(f"{pytest.test_dir}/step_8_inputs.yaml", "r") as fp:
        inputs = yaml.load(fp, Loader=yaml.FullLoader)
    with open(f"{pytest.test_dir}/step_8_expected_output.yaml", "r") as fp:
        outputs = yaml.load(fp, Loader=yaml.FullLoader)
    games_summoners_input = inputs["games_summoners_input"]
    summoners_list_input = inputs["summoners_list_input"]
    base_link = pytest.base_path_step_8
    games_summoners_df = pd.DataFrame.from_records(games_summoners_input)
    summoners_df = pd.DataFrame.from_records(summoners_list_input)
    # WHEN
    champions_details = st.step_8(
        logger=pytest.logger,
        header=pytest.header,
        base_link=base_link,
        players_df=summoners_df,
        games_players_df=games_summoners_df,
        verbose=pytest.verbose,
    )

    # THEN
    pd.testing.assert_frame_equal(champions_details, pd.DataFrame.from_records(outputs))


def test_step_9():
    # GIVEN

    with open(f"{pytest.test_dir}/step_9_expected_output.yaml", "r") as fp:
        outputs = yaml.load(fp, Loader=yaml.FullLoader)

    version_path = pytest.base_path_step_9_version
    champs_path = pytest.base_path_step_9_champs
    expected_output_df = pd.DataFrame.from_records(outputs)
    expected_output_df.sort_values("champion_name")

    # WHEN
    champions_details = st.step_9(
        logger=pytest.logger,
        header=pytest.header,
        version_link=version_path,
        champs_link=champs_path,
        verbose=pytest.verbose,
    )
    champions_details = champions_details[expected_output_df.columns]
    champions_details.sort_values("champion_name")
    # THEN
    pd.testing.assert_frame_equal(
        champions_details, expected_output_df, check_like=True
    )
