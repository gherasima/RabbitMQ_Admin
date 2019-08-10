import sys
import requests
from setting.config import *
import json
import argparse
from requests.exceptions import ConnectTimeout, MissingSchema, SSLError, Timeout, TooManyRedirects, ConnectionError

##############################################################################
#
#   Python 3.X is required
#
##############################################################################

# Check for Python minimum required version
if sys.version_info < required_python_min_version:
    print('Python minimum version ' + str(required_python_min_version) + ' is required!')
    sys.exit()

##############################################################################
#
#   General functions
#
##############################################################################


def die(message):
    print('\n [ ' + message + ' ]')
    sys.exit()

##############################################################################
#
#   Argument Parser
#
##############################################################################


def arg_parser():
    parser = argparse.ArgumentParser(description='Upgrading RabbitMQ with zero downtime')
    parser.add_argument("-j", "--json", help="A valid JSON file that contains all the required configuration details", type=str)
    parser.add_argument("-d", "--definitions", help="Export definitions JSON (queues, exchanges, bindings, users, \
                                                    virtual hosts, permissions, and parameters) from current RabbitMQ \
                                                    and importing them to the new RabbitMQ.", action="store_true")
    parser.add_argument("-m", "--messages", help="Using dynamic shovel move messages from current RabbitMQ to the \
                                                    new RabbitMQ.", action="store_true")
    args = parser.parse_args()
    if args.json:
        # Open JSON file
        with open(args.json) as json_file:
            data_dict = json.load(json_file)
            # Set env parameters
            # Set parameters
            datastore_path = data_dict["DATASTORE_PATH"]

    else:
        die('Please specify JSON file')
    if args.definitions:
        definitions = True
    if args.messages:
        messages = True

##############################################################################
#
#   Program Functions
#
##############################################################################


class ApiRequests:
    def __init__(self, rabbitmq_src, rabbitmq_user='guest', rabbitmq_password='guest', rabbitmq_port='15672',
                 amqp_port='5671', queue='', exchange='', shovel='', rabbitmq_dest='', vhost='vhost',
                 amqp_ssl= False, api_ssl=False, time_out=5,):
        self.rabbitmq_src = rabbitmq_src
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_password = rabbitmq_password
        self.rabbitmq_port = rabbitmq_port
        self.api_ssl = api_ssl
        self.amqp_ssl = amqp_ssl
        self.time_out = time_out
        self.shovel = shovel
        self.rabbitmq_dest = rabbitmq_dest
        self.queue = queue
        self.vhost = vhost
        self.amqp_port = amqp_port
        self.exchange = exchange

    def get_definitions(self):
        requests_exceptions = (ConnectTimeout, ConnectionError, MissingSchema, SSLError, Timeout, TooManyRedirects,
                               KeyError, UnicodeError)
        if self.api_ssl:
            http_type = 'https'
        else:
            http_type = 'http'
        url = http_type + '://' + self.rabbitmq_src + ':' + self.rabbitmq_port + '/api/definitions'
        try:
            r = requests.get(url, timeout=self.time_out, auth=(self.rabbitmq_user, self.rabbitmq_password))
            if 100 <= r.status_code <= 199:
                return 'Informational', r.status_code
            elif 200 <= r.status_code <= 299:
                return json.loads(r.text)
            elif 300 <= r.status_code <= 399:
                return 'Redirection', r.status_code
            elif 400 <= r.status_code <= 499:
                return 'Client Error', r.status_code
            elif 500 <= r.status_code <= 599:
                return 'Server Error', r.status_code
            else:
                return 'Can\'t connect!', r.status_code
        except requests_exceptions:
            return 'Can\'t connect!'

    def set_definitions(self, data):
        requests_exceptions = (ConnectTimeout, ConnectionError, MissingSchema, SSLError, Timeout, TooManyRedirects,
                               KeyError, UnicodeError)
        if self.api_ssl:
            http_type = 'https'
        else:
            http_type = 'http'
        url = http_type + '://' + self.rabbitmq_src + ':' + self.rabbitmq_port + '/api/definitions'
        try:
            r = requests.post(url, json=data, timeout=self.time_out, auth=(self.rabbitmq_user, self.rabbitmq_password))
            if 100 <= r.status_code <= 199:
                return 'Informational', r.status_code
            elif 200 <= r.status_code <= 299:
                return 'OK', r.status_code
            elif 300 <= r.status_code <= 399:
                return 'Redirection', r.status_code
            elif 400 <= r.status_code <= 499:
                return 'Client Error', r.status_code
            elif 500 <= r.status_code <= 599:
                return 'Server Error', r.status_code
            else:
                return 'Can\'t connect!', r.status_code
        except requests_exceptions:
            return 'Can\'t connect!'

    def replicate(self):
        requests_exceptions = (ConnectTimeout, ConnectionError, MissingSchema, SSLError, Timeout, TooManyRedirects,
                               KeyError, UnicodeError)
        if self.amqp_ssl:
            amqp_type = 'amqps'
        else:
            amqp_type = 'amqp'
        if self.api_ssl:
            http_type = 'https'
        else:
            http_type = 'http'
        url = http_type + '://' + self.rabbitmq_src + ':' + self.rabbitmq_port + '/api/parameters/shovel/%2f' + self.vhost + '/' + self.shovel
        data = {
            "component": "shovel",
            "name": self.shovel,
            "value": {
                "ack-mode": "on-confirm",
                "reconnect-delay": 5,
                "src-delete-after": "never",
                #"src-exchange-key": "",
                #"src-exchange": "",
                "src-queue": self.queue,
                "src-uri": amqp_type + "://" + self.rabbitmq_user + ':' + self.rabbitmq_password + '@' + self.rabbitmq_src + ':' + self.amqp_port + '/%2f' + self.vhost,
                "dest-uri": amqp_type + "://" + self.rabbitmq_user + ':' + self.rabbitmq_password + '@' + self.rabbitmq_dest + ':' + self.amqp_port + '/%2f' + self.vhost,

            },
            "vhost": "/" + self.vhost
        }
        print(data) # For debug only
        print(url)  # For debug only

        try:
            r = requests.put(url, json=data, timeout=self.time_out, auth=(self.rabbitmq_user, self.rabbitmq_password))
            if 100 <= r.status_code <= 199:
                return 'Informational', r.status_code, r.text
            elif 200 <= r.status_code <= 299:
                return 'OK', r.status_code, r.text
            elif 300 <= r.status_code <= 399:
                return 'Redirection', r.status_code, r.text
            elif 400 <= r.status_code <= 499:
                return 'Client Error', r.status_code, r.text
            elif 500 <= r.status_code <= 599:
                return 'Server Error', r.status_code, r.text
            else:
                return 'Can\'t connect!', r.status_code, r.text
        except requests_exceptions:
            return 'Can\'t connect!'

##############################################################################
#
#   Run program
#
##############################################################################

# Export definitions from source
#source = ApiRequests('130.138.11.142', 'admin', r'oisys!!')
#source_definitions = source.get_definitions()
#print(json.dumps(source_definitions, indent=4))     # For DEBUG only

# Import definitions to target
#target = ApiRequests('130.138.11.9', 'admin', r'oisys!!')
#print(target.set_definitions(source_definitions))


s = ApiRequests('130.138.11.142', 'admin', r'oisys!!', rabbitmq_port='15672',
                 queue='hello', shovel='admin', rabbitmq_dest='130.138.11.9', vhost='admin')
print(s.replicate())
