import pika

adriang_rabbitmq_01 = '130.138.11.142'
adriang_rabbitmq_02 = '130.138.11.9'
rabbitmq_user = 'admin'
rabbitmq_password = 'oisys!!'
rabbitmq_port = 5672
rabbitmq_vhost = 'admin'


def fill_queue(queue_name, messages, host, durable=False):
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    parameters = pika.ConnectionParameters(host,
                                           rabbitmq_port,
                                             rabbitmq_vhost,
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    # Create a hello queue
    channel.queue_declare(queue=queue_name, durable=durable)
    channel.basic_publish(exchange='', routing_key=queue_name, body=messages)
    connection.close()


def read_queue(queue_name, host, call_back):
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    parameters = pika.ConnectionParameters(host,
                                           rabbitmq_port,
                                           rabbitmq_vhost,
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    # Create a hello queue
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(call_back, queue=queue_name, no_ack=True)
    channel.start_consuming()


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


# Test
for r in range(1, 1000):
    print(r)
    fill_queue('hello', '12367', adriang_rabbitmq_01, durable=False)
#read_queue('hello', adriang_rabbitmq_01, callback)
