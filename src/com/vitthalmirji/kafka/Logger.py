import json
import logging

from kafka import KafkaProducer

logger_names = []


class Logger(logging.Handler):

    def __init__(self, Jobname, hostlist, topic, tls=None):
        self.__level = "INFO"
        self.__formatter = "%(asctime)s %(levelname)-8s %(message)s"
        self.__local_file_path = Jobname + ".log"
        logging.Handler.__init__(self)
        self.producer = KafkaProducer(bootstrap_servers=hostlist,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                      linger_ms=10)
        self.topic = topic

    def get(self, name):
        global logger_names
        logger = logging.getLogger(name)
        logger.setLevel(self.__level)
        if name not in logger_names:
            handler = logging.FileHandler(self.__local_file_path)
            formatter = logging.Formatter(self.__formatter)
            handler.setFormatter(formatter)
            handler.setLevel(self.__level)
            logger.addHandler(handler)
            logger_names.append(name)
        return logger

    # Write log to kafka topic
    def emit(self, record):
        # Avoid infinite loop by checking if Kafka's logs are looping in messages
        if 'kafka.' in record.name:
            return
        try:
            # apply the logger formatter
            msg = self.format(record)
            self.producer.send(self.topic, {'message': msg})
            self.flush(timeout=1.0)
        except Exception:
            logging.Handler.handleError(self, record)

    def flush(self, timeout=None):
        # Flush all the objects
        self.producer.flush(timeout=timeout)

    def close(self):
        # Close producer and clean up
        self.acquire()
        try:
            if self.producer:
                self.producer.close()
            logging.Handler.close(self)
        finally:
            self.release()
