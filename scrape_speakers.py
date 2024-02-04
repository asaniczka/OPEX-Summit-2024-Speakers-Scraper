"""This module will scrape https://www.processexcellencenetwork.com/events-opexweek/speakers for speakers at the event"""

import asyncio
import datetime
from urllib.parse import urlparse

import asaniczka
import pydantic
from bs4 import BeautifulSoup
from rich import print
import pandas as pd
import csv


class Speaker(pydantic.BaseModel):
    """Pydantic setup for speaker data"""

    name: str = None
    position: str = None
    company: str = None
    image: str = None
    bio: str = None
    venue: str = None
    day: datetime.date = None
    time: datetime.time = None
    topic: str = None
    topic_summary: str = None


def save_data(speakers: list[Speaker], PROJECT: asaniczka.ProjectSetup):
    """save data to file"""

    PROJECT.logger.info("Saving data as a csv")

    speakers = [x.model_dump() for x in speakers]

    df = pd.DataFrame(speakers)
    df.fillna(pd.NA, inplace=True)
    df.drop_duplicates(inplace=True)

    df.to_csv(
        f"{PROJECT.data_folder}/OPEX_speakers.csv", index=False, quoting=csv.QUOTE_ALL
    )


def extract_speaker_links(page: str, PROJECT: asaniczka.ProjectSetup) -> list[str]:
    """Extract speaker bio links"""

    soup = BeautifulSoup(page, "html.parser")

    speakers_el = soup.select(".media")
    speakers_profs = [x.select_one("a") for x in speakers_el]
    speakers_links = [x.get("href") for x in speakers_profs]

    return list(set(speakers_links))


def load_speaker_page(PROJECT: asaniczka.ProjectSetup) -> str:
    """load the speaker page for link extraction"""

    PROJECT.logger.info("Getting speaker page")

    url = "https://www.processexcellencenetwork.com/events-opexweek/speakers"

    response = asaniczka.get_request(url, logger=PROJECT.logger)

    return response


def extract_speaker_data(page: str, PROJECT: asaniczka.ProjectSetup) -> Speaker:
    """extracts speaker data from speaker bio page"""

    soup = BeautifulSoup(page, "html.parser")
    speaker = Speaker()

    speaker.name = soup.select_one(".mt-0.font-weight-light.d-inline").text
    speaker.position = soup.select_one(".title.font-weight-light.d-block.w-100").text
    speaker.company = soup.select_one(".company-field.font-weight-bold.d-block.w-100.pb-m-3").text  # fmt: skip
    speaker.image = soup.select_one("img.contributor-image").get("src")

    try:
        speaker.bio = soup.select_one(".media-body p").get_text(strip=True)
    except AttributeError:
        pass

    try:
        speaker.venue = (
            soup.select_one(".row.my-4.mx-0 > :first-child").text.split(":")[-1].strip()
        )
    except AttributeError:
        pass

    try:
        str_day = (
            soup.select_one(".row.my-4.mx-0 > :first-child")
            .text.split(":")[0]
            .split(",")[-1]
            .strip()
        ) + " 2024"
        speaker.day = datetime.datetime.strptime(str_day, "%b %d %Y").date()
    except AttributeError:
        pass

    try:
        str_time = (
            soup.select_one(
                ".speaker-session-list.col-12.py-1.my-1 .lead.font-weight-bold.w-100.d-block"
            )
            .get_text(strip=True, separator="|")
            .split("|")[0]
        )
        speaker.time = datetime.datetime.strptime(str_time, "%I:%M %p").time()
    except AttributeError:
        pass

    # fmt: off
    try:
        speaker.topic = soup.select_one(
                ".speaker-session-list.col-12.py-1.my-1 .lead.font-weight-bold.w-100.d-block"
            ).get_text(strip=True, separator="|").split("|")[-1]
    except AttributeError:
        pass

    try:
        speaker.topic_summary = soup.select_one("div.w-100.d-block")\
            .get_text(separator="\n", strip=True)
    except AttributeError:
        pass
    # fmt: on

    return speaker


async def load_speaker_bio_page(
    speaker_link: str, PROJECT: asaniczka.ProjectSetup
) -> str:
    """loads the speaker's bio page"""

    page = await asaniczka.async_get_request(speaker_link, PROJECT.logger)
    return page


async def handle_single_user(link: str, PROJECT) -> Speaker:
    """handle getting and parsing a single user"""

    url = urlparse(link)
    PROJECT.logger.info(f"Handling speaker: {url.path}")

    page = await load_speaker_bio_page(link, PROJECT)
    data = extract_speaker_data(page, PROJECT)

    return data


async def executor():
    """Main executor for this module"""

    PROJECT = asaniczka.ProjectSetup("OPEX_speakers_data")
    PROJECT.logger.info("Starting Executor")

    page = load_speaker_page(PROJECT)
    speaker_links: list[str] = extract_speaker_links(page, PROJECT)

    list_of_tasks = []
    for link in speaker_links:
        list_of_tasks.append(handle_single_user(link, PROJECT))

    results = await asyncio.gather(*list_of_tasks)

    save_data(results, PROJECT)

    PROJECT.logger.info(f"Finished Executor in {PROJECT.get_elapsed_time()}")


if __name__ == "__main__":
    asyncio.run(executor())
