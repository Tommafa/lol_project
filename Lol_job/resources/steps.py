import base_utils as bu
import pandas as pd
import riot_api_readers as riot_r
import json
import logging
import league_objects as lo
import os


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
    league_structure = bu.read_yaml(os.path.dirname(os.getcwd()) + config_yaml_path)[
        "league_structure"
    ]

    logger.info("loading base paths")
    # base links for Riot apis
    base_paths = bu.read_yaml(os.path.dirname(os.getcwd()) + config_yaml_path)[
        "base_links"
    ]
    logger.info("step 0 completed")

    return league_structure, base_paths, header


def step_1(
    logger: logging.Logger,
    header: dict,
    step_1_league_exp_v4_path: str,
    league_structure: dict,
    pages: int,
    verbose: bool = False,
):
    """Load from LEAGUE_EXP_V4 API the summoners for each rank/division"""

    # given the connection string, the Api header and the league
    # structure produces the dataset of the summoners
    logger.info("step 1 starting")

    summoners_dict, mini_series_dict = riot_r.get_summoners(
        logger,
        step_1_league_exp_v4_path,
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
    step_2_summoner_v4_path: str,
    header: dict,
    summonerId_column: str = "summonerId",
    verbose: bool = False,
):
    """Retrieve data from the SUMMONER_V4 API,
    to get the puuid of each loaded summoner at step 1."""
    # request for puuids through Riot API
    logger.info("step 2 starting")

    puuids_df = riot_r.get_puuids(
        logger=logger,
        base_link=step_2_summoner_v4_path,
        header=header,
        verbose=verbose,
        starting_df=summoners_pd,
        column_of_interest=summonerId_column,
    )
    logger.info("step 2 completed")
    return puuids_df


def step_3_4(
    logger: logging.Logger,
    header: dict,
    step_3_4_match_by_puuid_path: str,
    puuids_df: pd.DataFrame,
    games_to_load: int = 10,
    puuid_column: str = "puuid",
    historical_memory: int = 5,
    verbose: bool = False,
):
    """
    given a df of summoners with info,
    we use the MATCH_V5 API
    to get last M games played (only the game name)
    The outputs are:
    - a dictionary with the list
      of players for each game,
    - a set of games that represents
      the games that are potential t0(last game played)"""
    # request games list for each puuid
    logger.info("step 3_4 starting")
    (potential_t0_games, players_of_a_game) = riot_r.get_games(
        logger=logger,
        base_link=step_3_4_match_by_puuid_path,
        header=header,
        verbose=verbose,
        starting_df=puuids_df,
        column_of_interest=puuid_column,
        number_of_games=games_to_load,
        historical_memory=historical_memory,
    )
    logger.info("steps 3 and 4 completed")
    return players_of_a_game, potential_t0_games


def step_5(
    logger: logging.Logger,
    header: dict,
    base_link: str,
    players_of_a_game: dict,
    potential_t0_games: set,
    current_time: int,
    verbose: bool = False,
):
    """
    given:
    - a dictionary with the list
      of players for each game (players_of_a_game)
    - a set of games that represents
      the games that are potential t0(last game played)
    We read for each game in players_of_a_game,
    the details, if the game is in potential_t0_games
    we check that it's new enough: 12* 86400 is the equivalent
    of 12 days (remove it otherwise).
    We return:
    - The final set of potential_t0,
    - The list_of_missing_data with
      pair(game(in potential_t0), player)
      for each player not in players_of_a_game[game]
    - The dict Game_summoner (details of the summoners'
      results ingame)
    - The dict Game (details of the game)
    """
    logger.info("step 5 starting")
    list_of_missing_data = []
    games_details_list = []
    games_summoners_list = []
    potential_t0_games = set(potential_t0_games)
    for game_id in players_of_a_game:
        pt_0 = False
        (game_details) = riot_r.get_games_details(
            logger=logger,
            base_link=base_link,
            header=header,
            game_name=game_id,
            verbose=verbose,
        )

        if game_id in potential_t0_games:
            if current_time - game_details.info.gameCreation > 12 * 86400:  # 12 DAYS
                potential_t0_games.discard(game_id)

            else:

                list_of_missing_data.extend(
                    [
                        (game_id, el)
                        for el in set(game_details.players_list())
                        - set(players_of_a_game[game_id])
                    ]
                )
                pt_0 = True
        game_dict = game_details.get_game_dict()
        game_dict["potential_t0"] = pt_0
        games_details_list.append(game_dict)
        games_summoners_list.extend(game_details.get_game_summoner_dict())
    logger.info("step 5 completed")
    return (
        potential_t0_games,
        set(list_of_missing_data),
        games_details_list,
        games_summoners_list,
    )


def step_6(
    logger: logging.Logger,
    header: dict,
    players_games_list_endpoint: str,
    list_of_missing_data: set,
    game_type: str = "",
    historical_memory: int = 5,
    verbose: bool = False,
    number_of_games: int = 100,
):
    """
    Given a list of tuples list_of_missing_data(player, game_name):
    for each tuple we extract the last M games played by that player before
    the game_name.
    The function returns:
    - the list of games for which to retrive details
    - the list of players for which to retrive details
    """
    logger.info("step 6 starting")

    games_to_retrieve = set()
    players_to_retrieve = set()

    for i, game_puuid in enumerate(list_of_missing_data):
        game, puuid = game_puuid
        history_retrieved = False
        game_found = False
        games_to_skip = 0
        games_yet_to_find = historical_memory
        while not history_retrieved:
            retrieved_games = json.loads(
                riot_r.retrieve_last_n_games(
                    logger=logger,
                    base_link=players_games_list_endpoint,
                    puuid=puuid,
                    header=header,
                    verbose=verbose,
                    game_type=game_type,
                    games_to_skip=games_to_skip,
                    number_of_games=number_of_games,
                ).decode()
            )

            if game in retrieved_games:
                index_of_game = retrieved_games.index(game)
                games_to_retrieve.update(
                    retrieved_games[
                        index_of_game + 1 : index_of_game + games_yet_to_find + 1
                    ]
                )
                games_yet_to_find = max(
                    0, games_yet_to_find - len(retrieved_games) + index_of_game + 1
                )
                game_found = True
            elif game_found:
                games_to_retrieve.update(retrieved_games[:games_yet_to_find])
                break
            if games_yet_to_find == 0:
                history_retrieved = True

            games_to_skip += number_of_games
        players_to_retrieve.update([puuid])
    logger.info("step 6 starting")

    return players_to_retrieve, games_to_retrieve


def step_7(
    logger: logging.Logger,
    header: dict,
    base_link_games: str,
    base_link_puuid: str,
    base_link_encrypted_id: str,
    games_to_retrieve: set,
    summoners_to_retrieve: set,
    verbose: bool = False,
):
    """
    Given a list of games and players:
    1. retrieve games details,
    2. add players' names to the list of players,
    3. retrieve players encrypted id
    4. retrieve players' details
    5. return games and players with details
    """
    logger.info("step 7 starting")

    output_games_list = []
    output_games_summoners_list = []
    output_summoners_list = []
    for game_id in games_to_retrieve:
        (game_details) = riot_r.get_games_details(
            logger=logger,
            base_link=base_link_games,
            header=header,
            game_name=game_id,
            verbose=verbose,
        )
        summoners_to_retrieve.update(game_details.players_list())
        game_dict = game_details.get_game_dict()
        game_dict["potential_t0"] = False
        output_games_list.append(game_dict)
        output_games_summoners_list.extend(game_details.get_game_summoner_dict())

    for summoner in summoners_to_retrieve:
        response = eval(
            riot_r.retrieve_player_encrypted_id_from_puuid(
                logger, base_link_puuid, summoner, header, verbose
            )
            .decode()
            .replace("true", "True")
            .replace("false", "False")
        )
        player_details = eval(
            riot_r.retrieve_player_encrypted_id_from_puuid(
                logger, base_link_encrypted_id, response["id"], header, verbose
            )
            .decode()
            .replace("true", "True")
            .replace("false", "False")
        )

        player_details["puuid"] = response["puuid"]

        output_summoners_list.append(
            lo.LeagueEntryDTO(**player_details).full_dict(response["puuid"])
        )
    logger.info("step 7 completed")

    return output_games_list, output_games_summoners_list, output_summoners_list


def step_8(
    logger: logging.Logger,
    header: dict,
    base_link: str,
    players_df: pd.DataFrame,
    games_players_df: pd.DataFrame,
    verbose: bool = False,
):
    """
    Given a dataframe of players and a dataframe of games-players:
    - join the 2 dataframes on puuid
    - group by puuid and championId and compute winrate
    - make request to Champion-mastery-v4 api and retrieve for each puuid
    and champion the champion lvl
    - return a pandas df with columns: puuid, champ id, win rate, champ lvl
    """
    logger.info("step 8 starting")
    joined_df = players_df.merge(games_players_df, on="puuid")

    output_df = (
        joined_df.groupby(["puuid", "summonerId", "championId"])
        .mean(numeric_only=True)
        .round(2)["win"]
        .reset_index()
    )
    output_df["championLevel"] = output_df.apply(
        (
            lambda x: eval(
                riot_r.get_game_exp(
                    logger=logger,
                    base_link=base_link,
                    header=header,
                    player=x.summonerId,
                    champion=x.championId,
                    verbose=verbose,
                )
                .decode("utf-8")
                .replace("true", "True")
                .replace("false", "False")
            )["championLevel"]
        ),
        axis=1,
    )
    return output_df


def step_9(
    logger: logging.Logger,
    header: dict,
    version_link: str,
    champs_link: str,
    verbose: bool = False,
):
    """
    Load and return the champions' full list
    """
    logger.info("step 9 starting")
    latest_version = eval(
        bu.make_request(version_link, header, verbose, logger)
        .decode()
        .replace("true", "True")
        .replace("false", "False")
    )[0]
    logger.info(latest_version)
    logger.info(champs_link.format(latest_version))
    champions_dict = eval(
        bu.make_request(champs_link.format(latest_version), header, verbose, logger)
        .decode()
        .replace("true", "True")
        .replace("false", "False")
    )["data"]

    list_of_champs = [
        {"champion_name": key, "champion_id": champions_dict[key]["key"]}
        for key in champions_dict
    ]
    return pd.DataFrame.from_records(list_of_champs)
