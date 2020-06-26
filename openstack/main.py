# Guide flask:
#   https://www.digitalocean.com/community/tutorials/how-to-make-a-web-application-using-flask-in-python-3
#   https://flask-restful.readthedocs.io/en/latest/quickstart.html
#   https://techtutorialsx.com/2017/01/07/flask-parsing-json-data/
# sudo apt install -y python3-venv
# Posizionarsi nella cartella dove verrà sviluppato il codice:
# python3 -m venv sauron_env
# Per attivare l'ambiente appena creato:
# source sauron_env/bin/activate
# pip install flask
# pip install flask-restful

# Esempio richiesta
# Header:
'''
Content-Type:application/json
User-Agent:Mozilla
Accept:*/*
'''
# Body:
'''
{
	"image": "Cirros",
	"network": "internal",
	"flavor": "standard"
	"start": "Jun 26 2020 12:05PM"
	"end": "Jun 26 2020 12:06PM"
}
'''

import openstack
import sched, time
import datetime
import threading
from flask import Flask
from flask import request
from flask_restful import Resource, Api

IMAGE_NAME = "Cirros"
NETWORK_NAME = "internal"
FLAVOR_NAME = "standard"
START_PEAK = "Jun 26 2020 12:05PM"
END_PEAK = "Jun 26 2020 12:06PM"

# Begin Flask
app = Flask(__name__)
api = Api(app)


class OpenStackHandler(Resource):
    def post(self):
        if not request.is_json:
            return "Bad request", 400
        content = request.get_json()

        dateFormat = '%b %d %Y %I:%M%p'
        # compute the delay for the creation of the new server
        start_delay = (datetime.datetime.strptime(content['start'], dateFormat) - datetime.datetime.now()).total_seconds()
        print(start_delay)
        # compute the delay for the deletion of the new server
        end_delay = (datetime.datetime.strptime(content['end'], dateFormat) - datetime.datetime.now()).total_seconds()
        print(end_delay)

        # check if the date are antecedent the current date
        if start_delay < 0 or end_delay < 0 or content['start'] == content['end']:
            return "Bad request", 400

        server_schedule(content['image'], content['network'], content['flavor'], content['start'], content['end'], start_delay, end_delay)

        return "Success", 200


api.add_resource(OpenStackHandler, '/', methods=['POST'])
# End Flask

scheduler = None
conn = None


def create_server(image_name, network_name, flavor_name, start_peak, end_peak):
    global conn
    print("Create Server: serv:" + start_peak + "-" + end_peak)

    image = conn.compute.find_image(image_name)
    network = conn.network.find_network(network_name)
    flavor = conn.compute.find_flavor(flavor_name)

    # server.name create using start_peak and end_peak
    server = conn.compute.create_server(name="serv:" + start_peak + "-" + end_peak, image_id=image.id,
                                        flavor_id=flavor.id,
                                        networks=[{"uuid": network.id}])  # , key_name=keypair.name)

    server = conn.compute.wait_for_server(server)


def delete_server(start_peak, end_peak):
    global conn
    print("Delete Server: serv:" + start_peak + "-" + end_peak)

    server = conn.compute.find_server("serv:" + start_peak + "-" + end_peak)

    conn.compute.delete_server(server)


def create_flavors():
    global conn
    std = False
    lrg = False

    print("Creating flavors...")
    for f in conn.compute.flavors():
        if f.name == "large":
            std = True
        elif f.name == "standard":
            lrg = True
    if not std:
        std_f = conn.compute.create_flavor(name="standard", ram=128, vcpus=1, disk=1)
    if not lrg:
        lrg_f = conn.compute.create_flavor(name="large", ram=256, vcpus=2, disk=1)


def delete_flavors():
    global conn
    print("Deleting flavors...")
    for f in conn.compute.flavors():
        if f.name == "standard" or f.name == "large":
            conn.compute.delete_flavor(f.id)


def server_schedule(image, network, flavor, start_peak, end_peak, start_delay, end_delay):
    global conn
    global scheduler

    # schedule create and delete server
    scheduler.enter(start_delay, 1, create_server, (image, network, flavor, start_peak, end_peak,))
    scheduler.enter(end_delay, 2, delete_server, (start_peak, end_peak,))

    t = threading.Thread(target=scheduler.run)
    t.start()



if __name__ == '__main__':

    # Connect
    conn = openstack.connect()
    scheduler = sched.scheduler(time.time, time.sleep)

    create_flavors()

    print("Printing flavors:")
    for f in conn.compute.flavors():
        print(f.name)

    app.run(debug=True, host='0.0.0.0')
