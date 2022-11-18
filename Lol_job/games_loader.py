from __future__ import annotations

from resources import base_utils as bu
import pandas as pd
import resources.riot_api_readers as riot_r
from sqlalchemy import create_engine, schema, text, exc
import urllib
import os
from dotenv import load_dotenv
import logging


# TODO: get puuid via Summoner-v4 API - done
#       get match_ids via Match-V5 API - done
#       If top_n is None --> update summoners table adding puuid
#       get match stats via Match-V5 API
#       remove summoners_to_load
def main(
    logger: logging.Logger,
    env_config: str,
    db_password: str,
    db_user: str,
    api_key: str,
    db_schema: str,
    summoners_to_load: int | None = None,
    verbose: bool = True,
    summoner_id_column: str = "summonerId",
):

    config_yaml_path = env_config["config_path"]
    base_paths = bu.read_yaml(config_yaml_path)["base_links"]

    # if dev we can read the connection keys ( db and api) from a file,
    # otherwise they are secrets
    header = env_config["header"]
    header["X-Riot-Token"] = api_key

    db_settings = env_config["db_settings"]
    db_settings["username"] = db_user
    db_settings["password"] = db_password
    db_settings["schema"] = db_schema

    # setup connection
    quoted = urllib.parse.quote_plus(
        db_settings["connection_string"].format(
            db_settings["server"],
            db_settings["db_name"],
            db_settings["username"],
            db_settings["password"],
        )
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quoted}",
        fast_executemany=True,
    )

    if db_settings["schema"] not in engine.dialect.get_schema_names(engine):
        engine.execute(schema.CreateSchema(db_settings["schema"]))
        logger.info("a new schema was created")
    else:
        logger.info("schema already there")

    # If we want to limit loaded summoners, we will limit the
    # number to summoners_to_load
    top_n = f"top({summoners_to_load})" if summoners_to_load else ""
    # read summoners from table
    summoners_pd_from_sql = pd.read_sql(
        text(
            f"select {top_n} {summoner_id_column} from {db_settings['schema']}."
            f"{db_settings['summoners_table_name']}"
        ),
        con=engine,
    )

    # request for puuids through Riot API
    puuids_df = riot_r.get_puuids(
        logger=logger,
        base_link=base_paths["summoner_v4"],
        header=header,
        verbose=verbose,
        starting_df=summoners_pd_from_sql,
        column_of_interest=summoner_id_column,
    )

    logger.info("puuids loaded")

    logger.info(puuids_df)
    # request games list for each puuid
    games_ts = riot_r.get_games(
        logger=logger,
        base_link=base_paths["match_by_puuid"],
        header=header,
        verbose=verbose,
        starting_df=puuids_df,
        column_of_interest="puuid",
    )
    """games_list = []
    for puuid in puuids_df["puuid"]:
        games_list.extend(
            eval(
                riot_r.retrieve_last_n_games(
                    logger=logger,
                    base_link=base_paths["match_by_puuid"],
                    puuid=puuid,
                    header=header,
                    verbose=verbose,
                )
            )
        )"""
    logger.info(games_ts)
    engine.dispose()

    return summoners_pd_from_sql


if __name__ == "__main__":

    # read env file to understand where to find information
    try:
        env_conf = bu.read_yaml("resources/env_config.yaml")
        load_dotenv()

        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_USER = os.getenv("DB_USER")
        API_KEY = os.getenv("API_KEY")
        SCHEMA = os.getenv("SCHEMA")
        ENV = os.getenv("ENV")

        logger_instance = logging.getLogger(f"{__file__[:-2]}.log")
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filemode="w",
        )
        if ENV == "development":
            logger_instance.setLevel(logging.DEBUG)
        else:
            logger_instance.setLevel(logging.WARNING)
        main(
            logger_instance,
            env_conf,
            DB_PASSWORD,
            DB_USER,
            API_KEY,
            SCHEMA,
            summoners_to_load=10,
        )
    except exc.SQLAlchemyError as e:
        logger_instance.error(
            f"There was a problem with the db connection. {e}", exc_info=True
        )
    except Exception as e:
        logger_instance.error(f"There was an unexpected exception {e}", exc_info=True)
