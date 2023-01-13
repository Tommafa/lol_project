from resources import base_utils as bu
import pandas as pd
import riot_api_readers as riot_r

# from sqlalchemy import create_engine, schema, exc
# import urllib
# import os
# from dotenv import load_dotenv
import logging


# TODO: add on pipeline the logic of converting
#  number of summoners into number of pages 1+ n_summ//205
def step_0(logger: logging.Logger, env_config: dict, api_key):
    """setup of the configuration yamls +
    league structure + base paths for API requests"""
    logger.info("loading yaml")
    config_yaml_path = env_config["config_path"]

    logger.info("generating header")
    # setup header for Riot requests
    header = env_config["header"]
    header["X-Riot-Token"] = api_key

    logger.info("loading league structure")
    # used to obtain the lists of tiers and divisions
    league_structure = bu.read_yaml(config_yaml_path)["league_structure"]

    logger.info("loading base paths")
    # base links for Riot apis
    base_paths = bu.read_yaml(config_yaml_path)["base_links"]
    logger.info("step 0 completed")

    return league_structure, base_paths, header


def step_1(
    logger: logging.Logger,
    header: dict,
    base_paths: dict,
    league_structure: dict,
    pages: int,
    verbose: bool = False,
):
    """Load from LEAGUE_EXP_V4 API the summoners for each rank/division"""

    # given the connection string, the Api header and the league
    # structure produces the dataset of the summoners
    summoners_dict, mini_series_dict = riot_r.get_summoners(
        logger,
        base_paths["league_exp_v4"],
        league_structure["summoners_reader"],
        header,
        pages=pages,
        verbose=verbose,
    )

    logger.info("Summmoners retrieved from API")

    # merge dicts in a single one to add miniseries info for each summoner
    summoners_dict.update(mini_series_dict)

    # transform dict to pandas dataframe
    summoners_pd = pd.DataFrame.from_dict(summoners_dict)

    logger.info("step 1 completed")
    return summoners_pd


def step_2(
    logger: logging.Logger,
    summoners_pd: pd.DataFrame,
    base_paths: dict,
    header: dict,
    summonerId_column: str = "summonerId",
    verbose: bool = False,
):
    """Retrieve data from the SUMMONER_V4 API,
    to get the puuid of each loaded summoner at step 1."""
    # request for puuids through Riot API
    puuids_df = riot_r.get_puuids(
        logger=logger,
        base_link=base_paths["summoner_v4"],
        header=header,
        verbose=verbose,
        starting_df=summoners_pd,
        column_of_interest=summonerId_column,
    )
    logger.info("step 2 completed")
    return puuids_df


def step_3(
    logger: logging.Logger,
    header: dict,
    base_paths: dict,
    puuids_df: pd.DataFrame,
    games_to_load: int = 10,
    puuid_column: str = "puuid",
    verbose: bool = False,
):
    """For each puuid, use the MATCH_V5 API
    to get last M games played (only the game name)"""
    # request games list for each puuid
    (
        games_ts,
        games_of_a_player,
        players_of_a_game,
        list_of_missing_data,
    ) = riot_r.get_games(
        logger=logger,
        base_link=base_paths["match_by_puuid"],
        header=header,
        verbose=verbose,
        starting_df=puuids_df,
        column_of_interest=puuid_column,
        number_of_games=games_to_load,
    )
    logger.info("games' names list retrieved from riot API")
    logger.info("step 3 completed")
    return games_ts, games_of_a_player, players_of_a_game, list_of_missing_data


def step_4(
    logger: logging.Logger,
    header: dict,
    base_paths: dict,
    puuids_df: pd.DataFrame,
    games_to_load: int = 10,
    puuid_column: str = "puuid",
    verbose: bool = False,
):
    pass
