import openstack
# Connect
conn=openstack.connect()
# list images
for image in conn.compute.images():
	print(image)

print("Creating flavors...")
std_f = conn.compute.create_flavor(name='standard',ram=128, vcpus=1, disk=1)
lrg_f = conn.compute.create_flavor(name='large',ram=256, vcpus=2, disk=1)

print("Printing flavors:")
for f in conn.compute.flavors():
	print(f.name)

print("Deleting flavors...")
for f in conn.compute.flavors():
	if f.name == "standard" or f.name == "large":
		conn.compute.delete_flavor(f.id)

print("Printing flavors:")
for f in conn.compute.flavors():
	print(f.name)
