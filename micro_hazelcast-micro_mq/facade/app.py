import uuid
import pika
import requests
import consul_mapper

from flask import Flask, request

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def facade_service():
    if request.method == 'POST':
        post_msg_to_mq(request.json.get('message'))
        logging_service_response = requests.post(
            url=get_address("logging"),
            json={
                "uuid": str(uuid.uuid4()),
                "message": request.json.get('message')
            }
        )
        status = logging_service_response.status_code
        return app.response_class(status=status)
    else:
        logging_service_response = requests.get(get_address("logging"))
        print(logging_service_response)
        messages_service_r = requests.get(get_address("message"))
        return str(logging_service_response.text) + ' : ' + str(messages_service_r.text)


def post_msg_to_mq(msg: str):
    mq_connection = pika.BlockingConnection(
        pika.ConnectionParameters('127.0.0.1')
    )
    channel = mq_connection.channel()
    queue = consul_mapper.get_val("mq_name")
    channel.queue_declare(queue=queue)
    channel.basic_publish(
        exchange='', routing_key=queue,
        body=msg,
    )
    print(f"[x] Sent: {msg}")
    mq_connection.close()


def get_address(service_name):
    service = consul_mapper.get_service(service_name)
    print(service_name)
    return "http://192.168.0.101:" + str(service["ServicePort"]) + "/"


if __name__ == '__main__':
    port = consul_mapper.find_free_port()
    consul_mapper.register_self("facade", port)
    app.run(host='0.0.0.0', port=port)
