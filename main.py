import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable
from functools import reduce
from typing.io import TextIO
import asyncio
import time
import ijson

"""
Logging
"""


def initialise_global_logger():
    logger = logging.getLogger()
    file_handler = logging.FileHandler(filename="twitterHPC-" + str(datetime.now().strftime("%m.%d.%Y-%H:%M:%S"))
                                                + '.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)-6s --- [%(name)-18s] %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.setLevel("DEBUG")
    # Make this func called exactly once
    initialise_global_logger.__code__ = (lambda: None).__code__


# Inherit this class to enable logging
# Don't forget to Super call __init__ :
#   super().__init__(Class)
# Example Usage: self.logger.info()
class Loggable:
    def __init__(self, clazz):
        initialise_global_logger()
        self.logger = logging.getLogger(clazz.__name__)
        self.logger.addHandler(logging.NullHandler())


"""
Map handling
"""


class SentimentMap(Loggable):
    def __init__(self):
        super().__init__(SentimentMap)
        self.map = {}

    def load_from_file(self, path_to_file: str):
        with open(path_to_file, mode='r') as f:
            self.load_from_string(f.read(), silent=True)
            self.logger.info(self.load_from_file.__name__ + ": SentimentMap loaded from file " +
                             f.name + ", rows=" + str(len(self.map)))
        return self

    def load_from_string(self, data_str: str, silent: bool = False):
        data = list(map(lambda x: x.split("\t"), data_str.split("\n")))
        self.map = dict(data)
        if not silent:
            self.logger.info(self.load_from_file.__name__ + ": SentimentMap loaded from internal string"
                             + ", rows=" + str(str(len(self.map))))
        return self

    def __len__(self):
        return len(self.map)

    def __getitem__(self, item):
        return self.map[item]

    def __bool__(self):
        return self.map


class AreaMap(Loggable):
    def __init__(self):
        super().__init__(AreaMap)
        self.map = {}

    def load_from_file(self, path_to_file: str):
        with open(path_to_file, mode='r') as f:
            self.load_from_string(f.read(), silent=True)
            self.logger.info(self.load_from_file.__name__ + ": AreaMap loaded from file " +
                             f.name + ", rows=" + str(len(self.map)))
        return self

    def load_from_string(self, data_str: str, silent: bool = False):
        json_data = json.loads(data_str)
        self.map = {row["properties"]["id"]: {k: v for k, v in row["properties"].items() if k != "id"} for row in
                    json_data["features"]}
        if not silent:
            self.logger.info(self.load_from_file.__name__ + ": AreaMap loaded from internal string"
                             + ", rows=" + str(str(len(self.map))))
        return self

    def __len__(self):
        return len(self.map)

    def __getitem__(self, item):
        return self.map[item]

    def __bool__(self):
        return self.map


"""
Data handling
"""


class TwitterData(Loggable):
    def __init__(self):
        super().__init__(TwitterData)
        self.data = None
        self.len = 0
        self.offset = 0
        self.massive_file = None

    def load_from_file(self, path_to_file: str):
        with open(path_to_file, mode='r') as f:
            self.load_from_string(f.read(), silent=True)
            self.logger.info(self.load_from_file.__name__ + ": Data loaded from file " +
                             f.name + ", rows=" + str(self.len) + ", offset=" + str(self.offset))
        return self

    def load_from_file_massive(self, path_to_file: str):
        f = open(path_to_file, mode='r')
        print(f.readline())
        self.load_from_string_massive(f, silent=True)
        self.logger.info(self.load_from_file.__name__ + ": Data loaded from massive file " +
                         f.name + ", rows=" + str(self.len) + ", offset=" + str(self.offset))
        return self

    def load_from_string(self, data_str: str, silent: bool = False):
        json_data = json.loads(data_str)
        self.data = json_data["rows"]
        self.len = json_data["total_rows"]
        self.offset = json_data["offset"]
        if not silent:
            self.logger.info(self.load_from_file.__name__ + ": Data loaded from JSON string"
                             + ", rows=" + str(self.len) + ", offset=" + str(self.offset))
        return self

    def load_from_string_massive(self, file: TextIO, silent: bool = False):
        self.len = 1
        self.data = ijson.items(file, "rows.item")
        if not silent:
            self.logger.info(self.load_from_file.__name__ + ": Data loaded from massive JSON string"
                             + ", rows=" + str(self.len) + ", offset=" + str(self.offset))
        return self

    def close(self):
        if self.massive_file:
            self.massive_file.close()

    def __len__(self):
        return self.len


class TwitterPost(Loggable):
    def __init__(self):
        super().__init__(TwitterPost)
        self.text = None
        self.coordinates = None
        self.area = None


class TwitterPostFactory(Loggable):
    def __init__(self, sentiment_reducer: Callable[[TwitterPost, SentimentMap], int],
                 area_mapper: Callable[[TwitterPost, AreaMap], str], sentiment_map: SentimentMap, area_map: AreaMap):
        super().__init__(TwitterPostFactory)
        self.text_filters = []
        self.text_mappers = []
        self.sentiment_map = sentiment_map
        self.sentiment_reducer = sentiment_reducer
        self.area_map = area_map
        self.area_mapper = area_mapper

    def produce(self, data: dict) -> TwitterPost:
        twitter_post = TwitterPost()
        twitter_post.text = data['doc']['text'].split(" ")
        for func in self.text_mappers:
            twitter_post.text = list(map(func, twitter_post.text))
        for func in self.text_filters:
            twitter_post.text = list(filter(func, twitter_post.text))
        twitter_post.coordinates = data['value']['geometry']['coordinates']
        twitter_post.area = self.area_mapper(twitter_post, self.area_map)
        self.logger.debug(self.__init__.__name__ + ": Post parsed from data - {" + str(twitter_post.text)
                          + ", area = " + twitter_post.area + "}")
        return twitter_post

    def add_text_filter(self, text_filter: Callable[[str], bool]):
        self.text_filters.append(text_filter)

    def add_text_mapper(self, text_mapper: Callable[[str], str]):
        self.text_mappers.append(text_mapper)


def area_mapper(post: TwitterPost, area_map: AreaMap) -> str:
    potential_area = list(
        filter(lambda x: x[1]["xmax"] >= post.coordinates[0] >= x[1]["xmin"] and x[1]["ymax"] >= post.coordinates[1] >=
                         x[1]["ymin"],
               list(area_map.map.items())))
    if len(potential_area) > 0:
        post.logger.debug(
            area_mapper.__name__ + ": area parsed from coordinates " + str(post.coordinates) + " -> " +
            potential_area[0][0])
        return potential_area[0][0]
    else:
        return "outside"


if __name__ == "__main__":
    tw = TwitterData().load_from_file("smallTwitter.json")
    sentiment = SentimentMap().load_from_file("AFINN.txt")
    area = AreaMap().load_from_file("melbGrid.json")

    factory = TwitterPostFactory(lambda x, y: 0, area_mapper, sentiment_map=sentiment, area_map=area)
    factory.add_text_mapper(lambda x: x.lower())
    factory.add_text_filter(lambda x: "https://t.co/" not in x and "http://t.co/" not in x)

    # def run(item):
    #     factory.produce(item)

    # pool = ThreadPoolExecutor(max_workers=32)

    start = time.time()
    # for item in tw.data:
    #     pool.submit(run, item)
    for item in tw.data:
        factory.produce(item)

    # worker_num = 8
    # total_len = tw.len

    # for _ in range(worker_num):
    #     th = threading.Thread(target=run, args=[]).start()

    # pool.shutdown()
    print("Tasks done, time consumed = " + str(time.time()-start) + " seconds")

