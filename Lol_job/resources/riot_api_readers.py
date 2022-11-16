from typing import List, Dict
import requests
from resources.league_objects import LeagueEntryDTO, MiniSeriesDTO
import time


def build_summoners_links_per_division(
    config_summoners_reader: dict, headers: dict, verbose: bool = True
) -> List[dict]:
    """build list of tiers and divisions to load"""

    links_characteristics_list = []
    # define challenger dict
    challenger = dict(
        base_link=config_summoners_reader["base_path"],
        queue_type=config_summoners_reader["queue_type"],
        tier=config_summoners_reader["tiers"][0],
        division=config_summoners_reader["divisions"][0],
        page=1,
        headers=headers,
        verbose=verbose,
    )

    # define grandmaster dict
    grandmaster = dict(
        base_link=config_summoners_reader["base_path"],
        queue_type=config_summoners_reader["queue_type"],
        tier=config_summoners_reader["tiers"][1],
        division=config_summoners_reader["divisions"][0],
        page=1,
        headers=headers,
        verbose=verbose,
    )

    # define master dict
    master = dict(
        base_link=config_summoners_reader["base_path"],
        queue_type=config_summoners_reader["queue_type"],
        tier=config_summoners_reader["tiers"][2],
        division=config_summoners_reader["divisions"][0],
        page=1,
        headers=headers,
        verbose=verbose,
    )

    links_characteristics_list.extend([challenger, grandmaster, master])

    # define dicts for all the other leagues
    for i, tier in enumerate(config_summoners_reader["tiers"][3:]):
        for j, division in enumerate(config_summoners_reader["divisions"]):
            links_characteristics_list.extend(
                [
                    dict(
                        base_link=config_summoners_reader["base_path"],
                        queue_type=config_summoners_reader["queue_type"],
                        tier=tier,
                        division=division,
                        page=1,
                        headers=headers,
                        verbose=verbose,
                    )
                ]
            )

    return links_characteristics_list


def load_summoners_from_riot_api(
    base_link: str,
    queue_type: str,
    tier: str,
    division: str,
    page: int,
    headers: dict,
    verbose: bool = True,
) -> bytes:
    """load a list of summoners ordered by rank"""

    # fill link structure with input params
    link_for_request = f"{base_link}{queue_type}/{tier}/{division}?page={page}"

    # used to repeat the api call if we have code different from 200
    problems_at_previous_iteration = True
    iteration = 0
    while problems_at_previous_iteration and iteration < 5:
        # repeat up to 5 times
        try:

            response = requests.get(link_for_request, headers=headers)
            status_code = response.status_code
            if verbose:
                msg = (
                    "Everything went well!"
                    if status_code == 200
                    else "There was an error when handling the "
                    "request for the following link: {}.".format(link_for_request)
                )

                print(msg)
            if status_code == 200:
                problems_at_previous_iteration = False
                return response.content
            else:
                if verbose:
                    print(response.content)
                # usage limit is n calls every 2 minutes -->
                # when I get an error I wait 2 minutes as it will reset
                time.sleep(120)
                iteration += 1
        except Exception as e:
            iteration += 1
            print(
                "Unable to get url {} due to {}.".format(link_for_request, e.__class__)
            )


def load_list_of_summoners(
    configurations_for_summoners_reader: dict,
    header: dict,
    verbose: bool = True,
) -> tuple[Dict[str, list], Dict[str, list]]:
    """Retrieves a dataframe with values of each summoner"""
    # prepare dict for pandas df creation
    table_structure_summoners = {}
    table_structure_mini_series = {}
    for key in LeagueEntryDTO.schema()["properties"]:
        table_structure_summoners[key] = []

    for key in MiniSeriesDTO.schema()["properties"]:
        table_structure_mini_series[f"mini_series_{key}"] = []

        # create full list of leagues to load
        links_per_division = build_summoners_links_per_division(
            configurations_for_summoners_reader, header, verbose
        )
    print("table structure created")
    # for loop to: call the API, decode the bytes to string,
    # create the dict to append to our
    # initial pandas structure
    # ( the LeagueEntryDTO is necessary as sometimes
    # the miniSeries is absent..)
    for summoners in links_per_division:
        for summoner in eval(
            load_summoners_from_riot_api(**summoners)
            .decode("utf-8")
            .replace("true", "True")
            .replace("false", "False")
        ):
            league_entry = LeagueEntryDTO(**summoner).dict()
            mini_series_entry = MiniSeriesDTO(**league_entry["miniSeries"]).dict()
            for key in table_structure_summoners:
                table_structure_summoners[key].append(league_entry[key])
            for key in MiniSeriesDTO.schema()["properties"]:
                table_structure_mini_series[f"mini_series_{key}"].append(
                    mini_series_entry[key]
                )

    return table_structure_summoners, table_structure_mini_series


def load_puuid_for_summoner(logger, summoner: str, header: dict, verbose: bool = True):
    link_for_request = (
        f"https://euw1.api.riotgames.com/lol/summoner/v4/summoners/{summoner}"
    )

    # used to repeat the api call if we have code different from 200
    problems_at_previous_iteration = True
    iteration = 0
    while problems_at_previous_iteration and iteration < 5:
        # repeat up to 5 times
        try:

            response = requests.get(link_for_request, headers=header)
            status_code = response.status_code
            if verbose:
                msg = (
                    "Everything went well!"
                    if status_code == 200
                    else "There was an error when handling the "
                    f"request for the following link: {link_for_request}."
                )

                logger.info(msg)
            if status_code == 200:
                problems_at_previous_iteration = False
                return response.content
            else:
                if verbose:
                    logger.error(response.content)
                # usage limit is n calls every 2 minutes -->
                # when I get an error I wait 2 minutes as it will reset
                time.sleep(120)
                iteration += 1
        except Exception as e:
            iteration += 1
            logger.error(f"Unable to get url {link_for_request} due to {e.__class__}.")
