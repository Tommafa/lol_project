import urllib.parse as urlp
import pydantic
from typing import Optional
import urllib.request as urlreq
import bs4 as bs






def extract_summoners_list(pages: Optional[int], link: string ="https://www.op.gg/leaderboards/tier"):
    """Extracts top summoners from all passed pages
    """

    def extract_summoners_from_page(soup: object) -> list:
        "given a soup object it extracts summoner names' list"
        summoners_names = []
        for tbody in soup.find_all("tbody"):
            for link in tbody.find_all("a"):
                if link.get("href").startswith("/summoners"):
                    summoners_names.append(urlp.unquote(link.get("href").split("/")[-1]))
        return summoners_names

    summoner_names_list = []
    source = urlreq.urlopen(link).read()
    soup = bs.BeautifulSoup(source, 'lxml')
    summoner_names_list.extend(extract_summoners_from_page(soup))
    if pages:
        post_page_1 = "?page="
        for page in range(1, pages + 1):
            link_to_page = link + post_page_1 + "{}".format(page)
            source = urlreq.urlopen(link_to_page).read()
            soup = bs.BeautifulSoup(source, 'lxml')
            summoner_names_list.extend(extract_summoners_from_page(soup))

    return summoner_names_list