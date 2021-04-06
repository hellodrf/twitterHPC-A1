import json
import logging
from datetime import datetime
from typing import Callable
from functools import reduce
from typing.io import TextIO
import time
import ijson
import objgraph

"""
Logging
"""


def initialise_global_logger():
    logger = logging.getLogger()
    file_handler = logging.FileHandler(filename="twitterHPC-" + str(datetime.now().strftime("%m.%d.%Y-%H:%M:%S"))
                                                + '.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)-6s --- [%(name)-12s] %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    file_handler.setLevel("INFO")
    stream_handler.setLevel("INFO")
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
        # self.logger.addHandler(logging.NullHandler())


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
            self.data = self.load_from_string(f.read())
        self.logger.info(self.load_from_file.__name__ + ": Data loaded from file " +
                         path_to_file + ", rows=" + str(self.len) + ", offset=" + str(self.offset))
        return self

    def load_from_file_massive(self, path_to_file: str):
        f = open(path_to_file, mode='r')
        self.load_from_string_massive(f, silent=True)
        self.logger.info(self.load_from_file.__name__ + ": Data loaded from massive file " +
                         f.name + ", rows=" + str(self.len) + ", offset=" + str(self.offset))
        return self

    def load_from_string(self, data_str: str, silent: bool = False):
        json_data = json.loads(data_str)
        self.data = list(map(lambda x: x["value"], json_data["rows"]))
        self.len = json_data["total_rows"]
        self.offset = json_data["offset"]
        del json_data

        if not silent:
            self.logger.info(self.load_from_file.__name__ + ": Data loaded from JSON string"
                             + ", rows=" + str(self.len) + ", offset=" + str(self.offset))
        return self

    def load_from_string_massive(self, file: TextIO, silent: bool = False):
        # self.len = list(ijson.items(file, "total_rows"))[0]
        # file.seek(0, 0)
        self.data = ijson.items(file, "rows.item.value")
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
    def __init__(self, sentiment_reducer: Callable[[list, SentimentMap], int],
                 area_mapper: Callable[[list, AreaMap], str], sentiment_map: SentimentMap, area_map: AreaMap):
        super().__init__(TwitterPostFactory)
        self.text_filters = []
        self.text_mappers = []
        self.sentiment_map = sentiment_map
        self.sentiment_reducer = sentiment_reducer
        self.area_map = area_map
        self.area_mapper = area_mapper

    def produce(self, data: dict) -> (str, int):
        twitter_post = TwitterPost()
        twitter_post.text = data['properties']['text'].split(" ")
        for func in self.text_mappers:
            twitter_post.text = list(map(func, twitter_post.text))
        for func in self.text_filters:
            twitter_post.text = list(filter(func, twitter_post.text))
        twitter_post.area = self.area_mapper(data['geometry']['coordinates'], self.area_map)
        if not twitter_post.area:
            return None, 0
        twitter_post.score = self.sentiment_reducer(twitter_post.text, self.sentiment_map)
        self.logger.debug(self.__init__.__name__ + ": Post parsed from data - {score = " + str(twitter_post.score) +
                          ", text=" + str(twitter_post.text) + ", area = " + twitter_post.area + "}")
        del data
        return twitter_post.area, twitter_post.score

    def add_text_filter(self, text_filter: Callable[[str], bool]):
        self.text_filters.append(text_filter)

    def add_text_mapper(self, text_mapper: Callable[[str], str]):
        self.text_mappers.append(text_mapper)


def area_mapper(coordinates: list, area_map: AreaMap) -> str:
    potential_area = list(
        filter(lambda x: x[1]["xmax"] >= coordinates[0] >= x[1]["xmin"] and x[1]["ymax"] >= coordinates[1] >=
                         x[1]["ymin"],
               list(area_map.map.items())))
    if len(potential_area) > 0:
        logging.getLogger("TwitterPostFactory").debug(
            area_mapper.__name__ + ": area parsed from coordinates " + str(coordinates) + " -> " +
            potential_area[0][0])
        return potential_area[0][0]
    else:
        return ""


def sentiment_reducer(text: list, sentiment_map: SentimentMap) -> int:
    n = 0
    for i in text:
        n += int(sentiment_map[i])
    return n


if __name__ == "__main__":
    objgraph.show_growth()
    start = time.time()
    tw = TwitterData().load_from_file_massive("bigTwitter.json")
    sentiment = SentimentMap().load_from_file("AFINN.txt")
    area = AreaMap().load_from_file("melbGrid.json")

    factory = TwitterPostFactory(sentiment_reducer, area_mapper, sentiment_map=sentiment, area_map=area)
    factory.add_text_mapper(lambda x: x.lower().rstrip("!,?.’”"))
    factory.add_text_filter(lambda x: x in sentiment.map)

    logging.info(__name__ + ": Working...")

    result = {k: 0 for k in area.map}
    for item in tw.data:
        k, v = factory.produce(item)
        if k:
            result[k] += v

    logging.info(__name__ + ": Tasks done, time consumed = " + str(time.time() - start) + " seconds")
    logging.info(__name__ + ": Displaying result :" + str(result))
    print(__name__ + ": Displaying memory analysis :")
    objgraph.show_growth()
    tw.close()
