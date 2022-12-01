import logging
from typing import List, Dict

import pandas
import pandas as pd
from resources.league_objects import LeagueEntryDTO, MiniSeriesDTO
import resources.base_utils as bu


def build_summoners_links_per_division(
    base_path: str, config_summoners_reader: dict, header: dict, verbose: bool = True
) -> List[dict]:
    """build list of tiers and divisions to load"""

    links_characteristics_list = []
    # define challenger dict
    challenger = dict(
        base_link=base_path,
        queue_type=config_summoners_reader["queue_type"],
        tier=config_summoners_reader["tiers"][0],
        division=config_summoners_reader["divisions"][0],
        page=1,
        header=header,
        verbose=verbose,
    )

    # define grandmaster dict
    grandmaster = dict(
        base_link=base_path,
        queue_type=config_summoners_reader["queue_type"],
        tier=config_summoners_reader["tiers"][1],
        division=config_summoners_reader["divisions"][0],
        page=1,
        header=header,
        verbose=verbose,
    )

    # define master dict
    master = dict(
        base_link=base_path,
        queue_type=config_summoners_reader["queue_type"],
        tier=config_summoners_reader["tiers"][2],
        division=config_summoners_reader["divisions"][0],
        page=1,
        header=header,
        verbose=verbose,
    )

    links_characteristics_list.extend([challenger, grandmaster, master])

    # define dicts for all the other leagues
    for i, tier in enumerate(config_summoners_reader["tiers"][3:]):
        for j, division in enumerate(config_summoners_reader["divisions"]):
            links_characteristics_list.extend(
                [
                    dict(
                        base_link=base_path,
                        queue_type=config_summoners_reader["queue_type"],
                        tier=tier,
                        division=division,
                        page=1,
                        header=header,
                        verbose=verbose,
                    )
                ]
            )

    return links_characteristics_list


def get_summoners(
    logger: logging.Logger,
    base_path: str,
    configurations_for_summoners_reader: dict,
    header: dict,
    verbose: bool = True,
) -> tuple[Dict[str, list], Dict[str, list]]:
    """Starting from a given league structure,
    retrieves a dataframe with values of each summoner
     (only active ones!)"""

    # prepare dict for pandas df creation
    table_structure_summoners = {}
    table_structure_mini_series = {}
    for key in LeagueEntryDTO.schema()["properties"]:
        table_structure_summoners[key] = []

    for key in MiniSeriesDTO.schema()["properties"]:
        table_structure_mini_series[f"mini_series_{key}"] = []

    # create full list of leagues to load
    links_per_division = build_summoners_links_per_division(
        base_path, configurations_for_summoners_reader, header, verbose
    )
    logger.info("table structure created")
    # for loop to: call the API, decode the bytes to string,
    # create the dict to append to our
    # initial pandas structure
    # ( the LeagueEntryDTO is necessary as sometimes
    # the miniSeries is absent..)
    for division in links_per_division:
        for summoner in eval(
            load_summoners_from_riot_api(logger, **division)
            .decode("utf-8")
            .replace("true", "True")
            .replace("false", "False")
        ):
            league_entry = LeagueEntryDTO(**summoner).dict()
            mini_series_entry = MiniSeriesDTO(**league_entry["miniSeries"]).dict()
            if not league_entry["inactive"]:
                for key in table_structure_summoners:
                    table_structure_summoners[key].append(league_entry[key])
                for key in MiniSeriesDTO.schema()["properties"]:
                    table_structure_mini_series[f"mini_series_{key}"].append(
                        mini_series_entry[key]
                    )

    return table_structure_summoners, table_structure_mini_series


def get_puuids(
    logger: logging.Logger,
    base_link: str,
    header: dict,
    verbose: bool,
    starting_df: pd.DataFrame,
    column_of_interest: str,
) -> pd.DataFrame:
    """This function starts from a dataframe and returns
    a copy of that dataframe with the new column puuid"""
    output_df = starting_df.copy()
    puuids = pd.Series(
        [
            eval(
                load_puuid_for_summoner(
                    logger=logger,
                    base_link=base_link,
                    summoner=summoner,
                    header=header,
                    verbose=verbose,
                ).decode("utf-8")
            )["puuid"]
            for summoner in starting_df[column_of_interest]
        ]
    )
    output_df["puuid"] = puuids

    return output_df


def get_games(
    logger: logging.Logger,
    base_link: str,
    header: dict,
    verbose: bool,
    starting_df: pd.DataFrame,
    column_of_interest: str,
    number_of_games: int = 100,
) -> pd.Series:
    """given a summoners df with puuids it returns a
    list of the last 'number_of_games' games played for each puuid"""
    games = []
    time_ind = []
    for puuid in starting_df[column_of_interest]:
        list_of_games_per_puuid = eval(
            retrieve_last_n_games(
                logger=logger,
                base_link=base_link,
                puuid=puuid,
                header=header,
                verbose=verbose,
                number_of_games=number_of_games,
            ).decode("utf-8")
        )
        games.extend(list_of_games_per_puuid)
        time_ind.extend([i for i in range(len(list_of_games_per_puuid))])
    games = pd.Series(games, index=time_ind)
    return games


def get_games_details(
    logger: logging.Logger,
    base_link: str,
    header: dict,
    verbose: bool,
    games_list: pd.Series,
):
    games_details_list = []
    for game in games_list:
        link_for_request = f"{base_link}/{game}"
        games_details_list.append(
            bu.make_request(link_for_request, header, verbose, logger)
        )
    return games_details_list


def load_puuid_for_summoner(
    logger: logging.Logger,
    base_link: str,
    summoner: str,
    header: dict,
    verbose: bool = True,
) -> object:
    link_for_request = f"{base_link}/{summoner}"

    return bu.make_request(link_for_request, header, verbose, logger)


def load_summoners_from_riot_api(
    logger: logging.Logger,
    base_link: str,
    queue_type: str,
    tier: str,
    division: str,
    page: int,
    header: dict,
    verbose: bool = True,
) -> bytes:
    """load a list of summoners ordered by lp"""

    # fill link structure with input params
    link_for_request = f"{base_link}{queue_type}/{tier}/{division}?page={page}"
    return bu.make_request(link_for_request, header, verbose, logger)


def retrieve_last_n_games(
    logger: logging.Logger,
    base_link: str,
    puuid: str,
    header: dict,
    verbose: bool = True,
    game_type: str = "ranked",
    games_to_skip: int = 0,
    number_of_games: int = 100,
):
    link_for_request = (
        f"{base_link}/{puuid}/ids?type={game_type}&start={games_to_skip}"
        f"&count={number_of_games}"
    )
    return bu.make_request(link_for_request, header, verbose, logger)
