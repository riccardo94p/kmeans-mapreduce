import openstack
import sched, time
import datetime

IMAGE_NAME = "Cirros"
NETWORK_NAME = "internal"
FLAVOR_NAME = "standard"
START_PEAK = "Jun 26 2020 12:05PM"
END_PEAK = "Jun 26 2020 12:06PM"

scheduler = None

def create_server(conn):
    print("Create Server:")

    image = conn.compute.find_image(IMAGE_NAME)
    network = conn.network.find_network(NETWORK_NAME)
    flavor = conn.compute.find_flavor(FLAVOR_NAME)

    #server.name create using START_PEAK and END_PEAK
    server = conn.compute.create_server(name="serv:"+START_PEAK+"-"+END_PEAK, image_id=image.id, flavor_id=flavor.id,
                                        networks=[{"uuid": network.id}])  # , key_name=keypair.name)

    server = conn.compute.wait_for_server(server)
    print(server.name)

def delete_server(conn):
	print("Delete Server:")

	server = conn.compute.find_server("serv:"+START_PEAK+"-"+END_PEAK)

	print(server.name)

	conn.compute.delete_server(server)


def create_flavors(conn):
    std = False
    lrg = False

    print("Creating flavors...")
    for f in conn.compute.flavors():
        if f.name == "large":
            std = True
        elif f.name == "standard":
            lrg = True
    if std == False:
        std_f = conn.compute.create_flavor(name="standard", ram=128, vcpus=1, disk=1)
    if lrg == False:
        lrg_f = conn.compute.create_flavor(name="large", ram=256, vcpus=2, disk=1)


def delete_flavors(conn):
    print("Deleting flavors...")
    for f in conn.compute.flavors():
        if f.name == "standard" or f.name == "large":
            conn.compute.delete_flavor(f.id)


def server_schedule(conn):
	global scheduler
	format = '%b %d %Y %I:%M%p'
	#compute the delay for the creation of the new server
	s = (datetime.datetime.strptime(START_PEAK, format) - datetime.datetime.now()).total_seconds()
	print(s)
	#compute the delay for the deletion of the new server
	e = (datetime.datetime.strptime(END_PEAK, format) - datetime.datetime.now()).total_seconds()
	print(e)

	#check if the date are antecedent the current date
	if(s < 0 or e < 0):
		print("Input date not valid!")
		return

	#schedule create and delete server
	scheduler.enter(s, 1, create_server, (conn,))
	scheduler.enter(e, 1, delete_server, (conn,))
	scheduler.run()
	print("Creation and Deletion scheduled")

# Connect
conn = openstack.connect()
scheduler = sched.scheduler(time.time, time.sleep)

create_flavors(conn)

print("Printing flavors:")
for f in conn.compute.flavors():
    print(f.name)

# create_server(conn)
server_schedule(conn)

time.sleep(5)
print("Printing servers...")
for s in conn.compute.servers():
	print(s.name)



