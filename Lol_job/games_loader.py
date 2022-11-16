from resources import base_utils as bu
import pandas as pd
import resources.riot_api_readers as riot_r
from sqlalchemy import create_engine, schema, text
import urllib
import os
from dotenv import load_dotenv
import logging


# TODO: get puuid via Summoner-v4 API
#       get match_ids via Match-V5 API
#       get match stats via Match-V5 API
def main(logger, env_config, db_password, db_user, api_key, db_schema):
    config_yaml_path = env_config["config_path"]

    # if dev we can read the connection keys ( db and api) from a file,
    # otherwise they are secrets
    header = env_config["header"]
    header["X-Riot-Token"] = api_key

    db_settings = env_config["db_settings"]
    db_settings["username"] = db_user
    db_settings["password"] = db_password
    db_settings["schema"] = db_schema

    # used to obtain the lists of tiers and divisions
    db_actions = bu.read_yaml(config_yaml_path)["db_actions"]

    logger.info(
        db_settings["connection_string"].format(
            db_settings["server"],
            db_settings["db_name"],
            db_settings["username"],
            db_settings["password"],
        )
    )
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
        "mssql+pyodbc:///?odbc_connect={}".format(quoted),
        fast_executemany=True,
    )

    if db_settings["schema"] not in engine.dialect.get_schema_names(engine):
        engine.execute(schema.CreateSchema(db_settings["schema"]))
        logger.info("schema creato")
    else:
        logger.info("schema già presente")

    # read summoners from table
    summoners_pd_from_sql = pd.read_sql(
        text(
            db_actions["retrieve_from_table"].format(
                "summonerId", db_settings["schema"], db_settings["summoners_table_name"]
            )
        ),
        con=engine,
    )

    puuids = [
        eval(
            riot_r.load_puuid_for_summoner(
                logger=logger, summoner=summoner, header=header, verbose=True
            ).decode("utf-8")
        )["puuid"]
        for summoner in summoners_pd_from_sql["summonerId"].head()
    ]
    logger.info(puuids)
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
        main(logger_instance, env_conf, DB_PASSWORD, DB_USER, API_KEY, SCHEMA)
    except Exception as e:
        logger_instance.error(f"mancano le variabili d'ambiente {e}", exc_info=True)
