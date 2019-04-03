import configparser


class Config(object):
    """配置文件类"""

    def __init__(self, path, section=None):
        self.conf = configparser.ConfigParser()
        self.conf.read(path)
        self.section = section

    def __getattr__(self, item):
        return self.conf.get(self.section, item)
