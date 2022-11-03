from typing import List, Dict, Any

import request
import pydantic
import urllib.request as urlreq
import urllib.parse as urlp
from league_objects import *
import aiohttp
import json
import time


# TODO: change this function to organize it better
def build_summoners_links_per_division(config: dict, headers: dict,
                                       verbose: bool = True) -> List[dict]:
    """build list of tiers and divisions to load"""

    links_characteristics_list = []
    challenger = {"base_link": config["summoners_reader"]["base_path"],
                  "queue_type": config["summoners_reader"]["queue_type"],
                  "tier": config["summoners_reader"]["tiers"][0],
                  "division": config["summoners_reader"]["divisions"][0],
                  "page": 1,
                  "headers": headers,
                  "verbose": verbose

                  }
    grandmaster = {"base_link": config["summoners_reader"]["base_path"],
                   "queue_type": config["summoners_reader"]["queue_type"],
                   "tier": config["summoners_reader"]["tiers"][1],
                   "division": config["summoners_reader"]["divisions"][0],
                   "page": 1,
                   "headers": headers,
                   "verbose": verbose

                   }
    master = {"base_link": config["summoners_reader"]["base_path"],
              "queue_type": config["summoners_reader"]["queue_type"],
              "tier": config["summoners_reader"]["tiers"][2],
              "division": config["summoners_reader"]["divisions"][0],
              "page": 1,
              "headers": headers,
              "verbose": verbose

              }

    links_characteristics_list.extend([challenger, grandmaster, master])

    links_characteristics_list.extend([{"base_link": config["summoners_reader"]["base_path"],
                                        "queue_type": config["summoners_reader"]["queue_type"],
                                        "tier": tier,
                                        "division": division,
                                        "page": 1,
                                        "headers": headers,
                                        "verbose": verbose
                                        }
                                       for i, tier in enumerate(config["summoners_reader"]["tiers"][3:])
                                       for j, division in enumerate(config["summoners_reader"]["divisions"])])

    return links_characteristics_list

def load_summoners_from_riot_api(base_link: str, queue_type: str, tier: str, division: str, page: int,
                                 headers: dict,
                                 verbose: bool = True) -> str:
    """load a list of summoners ordered by rank"""

    link_for_request = "{}{}/{}/{}?page={}".format(base_link, queue_type, tier, division, str(page))
    problems_at_previous_iteration = True
    iteration = 0
    while problems_at_previous_iteration and iteration < 5:
        try:
            response = requests.get(link_for_request, headers=headers)
            status_code = response.status_code
            if verbose:
                msg = "Everything went well!" if status_code == 200 else "There was an error when handling the request for the following link: {}.".format(
                    link_for_request)

                print(msg)
            if status_code == 200:
                problems_at_previous_iteration = False
                return response.content
            else:
                if verbose:
                    print(response.content)
                time.sleep(120)

        except Exception as e:
            iteration += 1
            print("Unable to get url {} due to {}.".format(link_for_request, e.__class__))




def load_list_of_summoners(config: dict, header: dict, verbose: bool = True) -> List[LeagueEntryDTO]:
    links_per_division = build_summoners_links_per_division(config, header, verbose)
    result = []
    for url in links_per_division:
        summoners = eval(
            load_summoners_from_riot_api(**url).decode("utf-8").replace("true", "True").replace("false", "False"))
        for summoner in summoners:
            tmp = LeagueEntryDTO(**summoner)
            result.append(tmp)
    return result