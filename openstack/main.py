import openstack


IMAGE_NAME = "Cirros"
NETWORK_NAME = "internal"
FLAVOR_NAME = "standard"

def create_server(conn):
	print("Create Server:")

	image = conn.compute.find_image(IMAGE_NAME)
	network = conn.network.find_network(NETWORK_NAME)
	flavor = conn.compute.find_flavor(FLAVOR_NAME)

	server = conn.compute.create_server(name="test_serv", image_id=image.id, flavor_id=flavor.id, networks=[{"uuid": network.id}])#, key_name=keypair.name)

	server = conn.compute.wait_for_server(server)

def create_flavors(conn):
	std = False
	lrg = False
	
	print("Creating flavors...")
	for f in conn.compute.flavors():
		if f.name == "standard":
			std = True
		elif f.name == "large":
			lrg == True
	if std == False:
		std_f = conn.compute.create_flavor(name="standard",ram=128, vcpus=1, disk=1)
	if lrg == False:
		lrg_f = conn.compute.create_flavor(name="large",ram=256, vcpus=2, disk=1)

def delete_flavors(conn):
	print("Deleting flavors...")
	for f in conn.compute.flavors():
		if f.name == "standard" or f.name == "large":
			conn.compute.delete_flavor(f.id)
# Connect
conn = openstack.connect()

create_flavors(conn)

print("Printing flavors:")
for f in conn.compute.flavors():
	print(f.name)

create_server(conn)
print("Printing servers...")
for s in conn.compute.servers():
	print(s.name)
