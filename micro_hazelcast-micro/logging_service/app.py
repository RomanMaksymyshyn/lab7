import hazelcast
from flask import Flask, request
import consul_mapper

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def logger():
    if request.method == 'POST':
        print(f'\n --- post request from facade --- \n {request.json}\n')
        distributed_map = client.get_map('distr_map')
        distributed_map.set(str(request.json['uuid']), str(request.json['message']))
        print('--- SUCCESSFULLY SAVED ---')
        return app.response_class(status=200)
    else:
        distributed_map = client.get_map('distr_map')
        messages = distributed_map.values().result()
        print('\n --- get request from facade --- \n')
        return ','.join([msg for msg in messages]) or ''


if __name__ == '__main__':
    port = consul_mapper.find_free_port()
    service_id = consul_mapper.register_self("logging", port, "http://192.168.0.101")
    hz_addr = consul_mapper.get_val("hz/" + str(service_id))
    hz_cluster = consul_mapper.get_val("hz/cluster")
    client = hazelcast.HazelcastClient(
        cluster_name=hz_cluster,
        cluster_members=[hz_addr]
    )
    app.run(host='192.168.0.101', port=port)
