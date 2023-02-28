import logging
import os
import sys
from dotenv import load_dotenv
import steps as st
import base_utils as bu
import pandas as pd
import time
import urllib
from sqlalchemy import create_engine, schema, exc

sys.path.append("../resources")


def main(
    logger: logging.Logger,
    env_config: str,
    db_password: str,
    db_user: str,
    api_key: str,
    db_schema: str,
    summoners_to_load: int = None,
    verbose: bool = True,
    games_to_load: int = 100,
):
    current_time = int(time.time())
    league_structure, base_paths, header = st.step_0(
        logger=logger, env_config=env_config, api_key=api_key
    )
    logger.info("starting_steps")
    summoners_pd = st.step_1(
        logger=logger,
        header=header,
        step_1_league_exp_v4_path=base_paths["step_1_league_exp_v4"],
        league_structure=league_structure,
        pages=1 + summoners_to_load // env_config["number_of_players_per_page"],
        verbose=verbose,
    )
    puuids_df = st.step_2(
        logger=logger,
        summoners_pd=summoners_pd,
        step_2_summoner_v4_path=base_paths["step_2_7_summoner_v4"],
        header=header,
        verbose=verbose,
    )

    players_of_a_game, potential_t0_games = st.step_3_4(
        logger=logger,
        header=header,
        step_3_4_match_by_puuid_path=base_paths["step_3_4_6_match_by_puuid"],
        puuids_df=puuids_df,
        games_to_load=games_to_load,
        historical_memory=env_config["historical_memory"],
        verbose=verbose,
    )
    (
        potential_t0_games,
        list_of_missing_data,
        games_details_list,
        games_summoners_list,
    ) = st.step_5(
        logger=logger,
        header=header,
        base_link=base_paths["step_5_7_match_details"],
        players_of_a_game=players_of_a_game,
        potential_t0_games=potential_t0_games,
        current_time=current_time,
        verbose=verbose,
    )

    players_to_retrieve, games_to_retrieve = st.step_6(
        logger=logger,
        header=header,
        players_games_list_endpoint=base_paths["step_3_4_6_match_by_puuid"],
        list_of_missing_data=list_of_missing_data,
        verbose=verbose,
    )
    output_games_list, output_games_summoners_list, output_summoners_list = st.step_7(
        logger=logger,
        header=header,
        base_link_games=base_paths["step_5_7_match_details"],
        base_link_puuid=base_paths["step_7_summoner_v4_by_puuid"],
        base_link_encrypted_id=base_paths["step_2_7_summoner_v4"],
        games_to_retrieve=games_to_retrieve,
        summoners_to_retrieve=players_to_retrieve,
        verbose=verbose,
    )

    games_df = pd.DataFrame.from_records(games_details_list.extend(output_games_list))
    summoners_df = pd.concat(
        [puuids_df, pd.DataFrame.from_records(output_summoners_list)]
    )
    games_summoners_df = pd.DataFrame.from_records(
        games_summoners_list.extend(output_games_summoners_list)
    )

    champion_summoners_df = st.step_8(
        logger=logger,
        header=header,
        base_link=base_paths["step_8_champ_mastery_v4"],
        players_df=summoners_df,
        games_players_df=games_summoners_df,
        verbose=verbose,
    )

    champions_df = st.step_9(
        logger=logger,
        header=header,
        version_link=base_paths["step_9_version_link"],
        champs_link=base_paths["step_9_champions_link"],
        verbose=verbose,
    )

    logger.info("all steps are finished")

    logger.info("setting up db connection")

    db_settings = env_config["db_settings"]
    db_settings["username"] = db_user
    db_settings["password"] = db_password
    db_settings["schema"] = db_schema

    # setup connection string
    logger.info("setup of the connection string")
    quoted = urllib.parse.quote_plus(
        db_settings["connection_string"].format(
            db_settings["server"],
            db_settings["db_name"],
            db_settings["username"],
            db_settings["password"],
        )
    )

    logger.info("setup of the engine")
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quoted}",
        fast_executemany=True,
    )

    # verify schema and create it eventually
    if db_settings["schema"] not in engine.dialect.get_schema_names(engine):
        engine.execute(schema.CreateSchema(db_settings["schema"]))
        logger.info("A new schema was created")
    else:
        logger.info("Schema already there")

    logger.info("writing df content to db")

    summoners_df.to_sql(
        "summoners",
        schema=db_settings["schema"],
        con=engine,
        if_exists="replace",
        index=False,
        index_label="summonerId",
        method="multi",
        chunksize=100,
    )
    games_summoners_df.to_sql(
        "tmp_games_summoners",
        schema=db_settings["schema"],
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=100,
    )
    games_df.to_sql(
        "games",
        schema=db_settings["schema"],
        con=engine,
        if_exists="replace",
        index=False,
        index_label="summonerId",
        method="multi",
        chunksize=100,
    )
    champion_summoners_df.to_sql(
        "tmp_champions_summoners",
        schema=db_settings["schema"],
        con=engine,
        if_exists="replace",
        index=False,
        index_label="summonerId",
        method="multi",
        chunksize=100,
    )
    champions_df.to_sql(
        "champions",
        schema=db_settings["schema"],
        con=engine,
        if_exists="replace",
        index=False,
        index_label="summonerId",
        method="multi",
        chunksize=100,
    )


if __name__ == "__main__":
    # setup the logger instance
    log_inst = logging.getLogger("pipeline_logger")
    log_inst.setLevel(logging.DEBUG)

    # set the file where we write the logs
    handler = logging.FileHandler(f"{__file__[:-3]}.log")
    handler.setLevel(logging.INFO)

    # the format for the logs
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # setting console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # add formatter to handlers
    console_handler.setFormatter(formatter)
    handler.setFormatter(formatter)

    log_inst.addHandler(handler)
    log_inst.addHandler(console_handler)

    log_inst.info("starting now")
    try:
        # the config file contains basic db configuration and
        # the basic header
        env_conf = bu.read_yaml(
            os.path.dirname(os.getcwd()) + "/resources/env_config.yaml"
        )

        # this command loads the content of the .env file
        # as env variables
        load_dotenv()

        # read all the info from the env
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_USER = os.getenv("DB_USER")
        API_KEY = os.getenv("API_KEY")
        SCHEMA = os.getenv("SCHEMA")
        ENV = os.getenv("ENV")

        # defining the logging level based on the env
        # if ENV == "development":
        #     handler.setLevel(logging.DEBUG)
        # else:
        #     handler.setLevel(logging.WARNING)

        # adding the settings to the log instance

        main(log_inst, env_conf, DB_PASSWORD, DB_USER, API_KEY, SCHEMA, 100, True, 6)
    except exc.SQLAlchemyError as e:
        log_inst.error(
            f"There was a problem with the db connection. {e}", exc_info=True
        )
    except AttributeError as e:
        log_inst.error(f"there was a problem with the request: {e}", exc_info=True)
    except Exception as e:
        log_inst.error(f"current wd: {os.getcwd()}")
        log_inst.error(f"There was an unexpected exception {e}", exc_info=True)
