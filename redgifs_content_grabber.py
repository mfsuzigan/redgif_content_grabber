import argparse
import datetime
import logging
import math
import os
import threading
import time

import requests
from bs4 import BeautifulSoup
from requests import RequestException
from selenium.common import TimeoutException
from selenium.webdriver import Chrome
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium import webdriver
from selenium.webdriver.common.by import By

OUTPUT_DIR = None
WEBDRIVER_RENDER_TIMEOUT_SECONDS = 10
THREADS_TO_USE = 10
MAX_REQUEST_RETRIES = 5

driver: Chrome
args: argparse.Namespace
captured_links = set()
file_cues = set()
downloaded_files_sizes_mb = []


def get_captured_links_batches():
    file_batch_size = math.ceil(len(captured_links) / THREADS_TO_USE)
    batches = []

    while len(captured_links) > 0:
        batches.append([captured_links.pop() for _ in range(file_batch_size) if len(captured_links) > 0])

    return batches


def get_file_saving_threads(captured_links_batches):
    return [threading.Thread(target=(lambda files: [save_file(f) for f in files]), args=(batch,))
            for batch in captured_links_batches]


def is_duplicate(file_path):
    return os.path.exists(file_path)


def build_real_url(file_name):
    return f"https://api.redgifs.com/v2/gifs/{file_name.lower()}/files/{file_name}.mp4"


def extract_file_name(url):
    name_parts = url.split("/")
    name = "ERROR"

    if len(name_parts) > 1:
        name = name_parts[-1].split('?')[0]
        name = name.split('#')[0]
        name = name.replace("-mobile", "")
        name = name.replace("-silent", "")
        name = name.replace("-large", "")
        name = name.split(".")[0]

    return name


def save_file(input_):
    file_name = input_ if args.mode == "s" else extract_file_name(input_)

    if ".jpg" in input_:
        true_url = input_
        file_name += ".jpg"

    else:
        true_url = build_real_url(file_name)
        file_name += ".mp4"

    file_path = f"{OUTPUT_DIR}/{file_name}"
    thread_id = threading.currentThread().ident
    is_successful = False

    if is_duplicate(file_path):
        logging.info(f"T-{thread_id}: Skipping duplicate file by content: {file_name}")

    else:
        try:
            with open(file_path, "wb") as file:
                logging.info(f"T-{thread_id}: Downloading file: {file_name}")
                file.write(safely_request_content(true_url))
                downloaded_files_sizes_mb.append(os.path.getsize(file_path) / 1024 ** 2)
                is_successful = True

        except OSError as e:
            logging.error(f"T-{thread_id}: Error writing file: {file_name}", e)

    return is_successful


def safely_request_content(url):
    successful = False
    content = ""

    for _ in range(MAX_REQUEST_RETRIES):

        if successful:
            break

        else:
            try:
                content = requests.get(url).content
                successful = True

            except RequestException:
                logging.warning(f"Retrying download of {url}")

    return content


def centralize_at_element(element):
    driver.execute_script("arguments[0].scrollIntoView(true);", element)


def wait_until_visible(locator):
    wait = WebDriverWait(driver, WEBDRIVER_RENDER_TIMEOUT_SECONDS)

    try:
        wait.until(ec.visibility_of_element_located(locator))
    except TimeoutException:
        pass


def capture_content_links(url):
    driver.get(url)
    wait_until_visible((By.CLASS_NAME, "gifList.userGifList"))
    video_tiles = driver.find_elements(By.CLASS_NAME, "tile.isVideo")
    captured_tiles = []

    while len(captured_tiles) != len(video_tiles):
        tiles_to_inspect = [t for t in video_tiles if t not in captured_tiles]

        for tile in tiles_to_inspect:
            centralize_at_element(tile)
            src = tile.find_element(By.CLASS_NAME, "thumbnail").get_attribute("src")

            if args.mode == "p":
                tile.screenshot(f"{OUTPUT_DIR}/{src.split('/')[-1].split('?')[0]}.png")

            else:
                captured_links.add(src)

        captured_tiles.extend(tiles_to_inspect)
        video_tiles = driver.find_elements(By.CLASS_NAME, "tile.isVideo")


def setup_output_directory(identifier):
    logging.info("Setting up directory")

    global OUTPUT_DIR
    OUTPUT_DIR = f"{args.output}/{identifier if identifier else ''}"
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_identifier(target):
    identifier = None

    if target:
        identifier_parts = target.split("/")

        if len(identifier_parts) > 1:
            identifier = identifier_parts[-1].split("?")[0]

    else:
        logging.warning(f"Could get identifier from target {target}")

    return identifier


def build_capture_links_from_cues():
    captured_links.update([file_name.split("-")[0] for file_name in os.listdir(OUTPUT_DIR)])


def build_capture_links_from_text_file():
    preliminary_links = []

    with open(f"{OUTPUT_DIR}/input.txt") as input_file:
        preliminary_links.extend([f"https://www.{link}" for link in input_file.read().split("https://www.")][1:])

    for link in preliminary_links:
        logging.info(f"Capturing true link for {link}")
        soup = BeautifulSoup(safely_request_content(link), "html.parser")
        src = soup.find("meta", {"property": "og:video"})

        if not src:
            src = soup.find("meta", {"property": "og:image:url"})

        if src:
            captured_links.add(src.attrs["content"])


def main():
    start = time.time()
    logging.getLogger().setLevel(logging.INFO)

    global args
    args = get_args()
    webdriver_setup()
    identifier = get_identifier(args.target)
    setup_output_directory(identifier)

    if args.mode == "f":
        build_capture_links_from_text_file()

    elif args.mode == "s":
        build_capture_links_from_cues()

    else:
        capture_content_links(args.target)

    save_files()

    logging.info(f"\nDone in {datetime.timedelta(seconds = (time.time() - start))}. "
                 f"Downloaded content: {len(downloaded_files_sizes_mb)} files "
                 f"({sum(downloaded_files_sizes_mb):.2f} MB)")


def save_files():
    threads = [threading.Thread(target=(lambda files: [save_file(f) for f in files]), args=(batch,))
               for batch in get_captured_links_batches()]

    logging.info("Starting file saving threads")
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]


def get_args():
    logging.info("Reading args")
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--target", "-t", required=False, help="Target page")
    arg_parser.add_argument("--output", "-o", required=True, help="Output directory")
    arg_parser.add_argument("--headless", "-hl", action="store_true", help="Headless run (flag)")
    arg_parser.add_argument("--mode", "-m", default="v", help="Mode: v, t, f, s (video, thumbs, "
                                                              "from text file or selected from directory)")

    return arg_parser.parse_args()


def webdriver_setup():
    if args.mode in ["v", "p"]:
        logging.info("Setting up webdriver")
        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument('--disable-dev-shm-usage')

        if args.headless:
            options.add_argument("--headless")

        global driver
        driver = webdriver.Chrome(options=options)


if __name__ == "__main__":
    main()
