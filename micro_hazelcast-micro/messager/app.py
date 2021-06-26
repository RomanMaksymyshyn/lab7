import sys
import threading

import pika
from flask import Flask
import consul_mapper

app = Flask(__name__)


@app.route('/',  methods=['GET'])
def messages():
    print(ALL_TIME_MESSAGES_11)
    return str(ALL_TIME_MESSAGES_11)


def threaded(fn):
    def run(*args, **kwargs):
        t = threading.Thread(target=fn, args=args, kwargs=kwargs)
        t.start()
        return t
    return run

@threaded
def consuming(messages_list):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1')
    )
    channel = connection.channel()
    queue = consul_mapper.get_val("mq_name")
    channel.queue_declare(queue=queue)
    def callback(ch, method, properties, body):    
        print(" [x] Received %r" % body.decode())
        messages_list.append(body.decode())
        print(messages_list)
    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    address = sys.argv[0]
    port = consul_mapper.find_free_port()
    consul_mapper.register_self("message", port, "http://" + address)
    ALL_TIME_MESSAGES_11 = []
    consuming(ALL_TIME_MESSAGES_11)
    print('LETS GO 1')
    app.run(host='192.168.0.101', port=port, debug=False)
