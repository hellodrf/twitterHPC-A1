import json
import logging
from datetime import datetime

"""
Logging
"""


def initialise_global_logger():
    logger = logging.getLogger()
    file_handler = logging.FileHandler(filename="twitterHPC-" + str(datetime.now().strftime("%m.%d.%Y-%H:%M:%S"))
                                                + '.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)-6s --- [%(name)-10s] %(message)s')
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
Data handling
"""


class TwitterData(Loggable):
    def __init__(self):
        super().__init__(TwitterData)
        self.data = None
        self.len = 0
        self.offset = 0

    def load_from_file(self, path_to_file: str):
        with open(path_to_file, mode='r') as f:
            self.load_from_string(f.read(), silent=True)
            self.logger.info(self.load_from_file.__name__ + ": Data loaded from file " +
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

    def __len__(self):
        return self.len


class TwitterPost(Loggable):
    def __init__(self):
        super().__init__(TwitterPost)
        self.text = None
        self.coordinates = None
        self.area = None


class TwitterPostFactory(Loggable):
    def __init__(self, sentiment_mapper, area_mapper, sentiment_map: dict, area_map: dict):
        super().__init__(TwitterPostFactory)
        self.sentiment_map = sentiment_map
        self.sentiment_mapper = sentiment_mapper
        self.area_map = area_map
        self.area_mapper = area_mapper

    def produce(self, data: dict):
        twitter_post = TwitterPost()
        twitter_post.text = list(map(lambda x: x.lower(), data['doc']['text'].split(" ")))
        twitter_post.coordinates = data['doc']['coordinates']['coordinates']
        twitter_post.logger.debug(self.__init__.__name__ + ": Post parsed from data - {\"" + str(twitter_post.text) + "\", " + str(
            twitter_post.coordinates) + "}")
        return twitter_post


if __name__ == "__main__":
    tw = TwitterData().load_from_file("tinyTwitter.json")
    factory = TwitterPostFactory(None, None, {}, {})

    for item in tw.data:
        post = factory.produce(item)
