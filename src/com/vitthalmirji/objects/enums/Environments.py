import abc
import socket

from pyspark.sql import SparkSession


class Environment:
    def __init__(self, name, nameservice, zookeeperquorum, historyserver):
        self.name = name
        self.nameservice = nameservice
        self.zookeeperquorum = zookeeperquorum
        self.historyserver = historyserver


class IEnvironment:
    @abc.abstractmethod
    def getEnvironment(self, sc: SparkSession): Environment

    def getEnvironmentByServer(self): Environment


class Environments(IEnvironment):
    def __init__(self):
        self.local = Environment("local", "", "localhost", "localhost:18081")
        self.dev = Environment("dev", "", "localhost", "localhost:18081")
        self.intg = Environment("intg", "", "localhost", "localhost:18081")
        self.test = Environment("test", "", "localhost", "localhost:18081")
        self.prod = Environment("prod", "", "localhost", "localhost:18081")

    def getEnvironment(self, sc: SparkSession) -> Environment:
        hostname = socket.gethostname()
        if hostname.lower().startswith("v") or hostname.lower().startswith("u"):
            return self.local
        elif hostname.lower().startswith("intg"):
            return self.intg
        elif hostname.lower().startswith("test"):
            return self.test
        elif hostname.lower().startswith("prod"):
            return self.prod

    def getEnvironmentByServer(self) -> Environment:
        hostname = socket.gethostname()
        if hostname.lower().startswith("v") or hostname.lower().startswith("u"):
            return self.local
        elif hostname.lower().startswith("intg"):
            return self.intg
        elif hostname.lower().startswith("test"):
            return self.test
        elif hostname.lower().startswith("prod"):
            return self.prod
