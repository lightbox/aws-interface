#!/usr/bin/env python
# Script the creation of various EC2 servers

import time

from fabric.api import *
from boto.ec2.connection import EC2Connection

sys.path.append('/etc')
from lightboxkeys import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

######################################
## Fill in this section
AWS_ZONE = "" #e.g. 'us-east-1a'
OWNER_ID = ""
DEFAULT_KEY_PAIR = ""
DEFAULT_SECURITY_GROUP = [""]
# Don't image servers that might be serving traffic. Also, they could be
# compromised.
DO_NOT_IMAGE = [] #machine name list

######################################

SIZE = {'small': 'm1.small',
       }

RIGHTIMAGE_AMI_32 = "ami-a8f607c1"
RIGHTIMAGE_AMI_64 = "ami-aef607c7"
AMI = {'small': RIGHTIMAGE_AMI_32,
       'large': RIGHTIMAGE_AMI_64,
      }

conn = None

def image_server_by_name(server_name, no_reboot=False):
    """Snapshot a server, given name
    """

    conn = connect_aws()

    # Match the given name to an instance
    reservations = conn.get_all_instances()
    all_instances = [r.instances[0] for r in reservations]
    all_instance_names = [get_instance_name(inst.tags) for inst in all_instances]

    instance_dict = dict(zip(all_instance_names, all_instances))

    try:
        instance = instance_dict[server_name]
    except KeyError:
        print 'Server %s not found' % server_name
        raise

    image_server_by_id(instance.id, no_reboot)

def image_server_by_id(instance_id, no_reboot=False):
    """Snapshot a server, given instace_id
    """

    conn = connect_aws()

    instance = conn.get_all_instances([instance_id])[0].instances[0]
    instance_name = get_instance_name(instance.tags)

    if instance_name in DO_NOT_IMAGE:
        print "%s should not be imaged" % instance_name
        raise ValueError("Invalid image")

    all_images = conn.get_all_images(owners=[OWNER_ID])
    all_image_names = [image.name for image in all_images]

    new_image_name = instance_name + time.strftime('-%Y-%m-%d')

    if new_image_name in all_image_names:
        count = 0
        while new_image_name + str(count) in all_image_names:
            count +=1
        new_image_name = new_image_name + str(count)

    image = conn.create_image(instance.id, new_image_name, no_reboot)

    wait_for_aws(image, "pending")

    return image

def build_web_server(type='m1.medium', name='web test'):
    """Build a standard webnode
    """

    print "Creating a %s instance with name \"%s\"" % (type, name)

    connect_aws()

    web_server_settings = {
                    'ami_id': RIGHTIMAGE_AMI_64, 
                    'zone': AWS_ZONE,
                    'security_groups': DEFAULT_SECURITY_GROUP,
                    'key_pair': DEFAULT_KEY_PAIR,
                    'instance_type': type,
                    'instance_name': name,
                }

    instance = create_instance(web_server_settings)

    web_server_settings['instance_id'] = instance.id
    web_server_settings['ip_address'] = instance.public_dns_name

    print "Created Web Server %(instance_name)s with id %(instance_id)s at %(ip_address)s" % web_server_settings

def build_log_server(name='logs', size='small', create_new_volume=False):
    """Build a standard log server
    """

    connect_aws()

    ami_id = AMI[size]
    instance_type = SIZE[size]

    log_server_settings = {
                    'ami_id': ami_id,
                    'zone': AWS_ZONE,
                    'security_groups': DEFAULT_SECURITY_GROUP,
                    'key_pair': DEFAULT_KEY_PAIR,
                    'instance_type': instance_type,
                    'instance_name': name,
                    'volume_size': '1000',
                    'volume_name': 'logs',
                    'mount_point': '/dev/sdk',
                }

    instance = create_instance(log_server_settings)
    instance.add_tag('Name', 'logs')
    if create_new_volume:
        add_volume(instance, log_server_settings)

    print "Created Logging Server, id: %s at %s" % (instance.id,
            instance.public_dns_name)

def connect_server(instance_id):
    """Connect to server with instance_id, set as fabric host
    """
    conn = connect_aws()
    instance = conn.get_instance(instance_id)
    env.hosts = [instance.public_dns_name]

def create_instance(inst_settings):
    """Create an instance with given settings
    """

    image_name = inst_settings['ami_id']
    run_settings = {'placement': inst_settings['zone'],
                    'key_name': inst_settings['key_pair'],
                    'instance_type': inst_settings['instance_type'],
                    'security_groups': inst_settings['security_groups'],
                    }

    reservation = conn.run_instances(image_name, **run_settings)
    instance = reservation.instances[0]
    instance.add_tag('Name', inst_settings['instance_name'])

    wait_for_aws(instance, "pending")

    return instance

def add_volume(instance, inst_settings):
    """Attach a volume to instance
    """

    size = inst_settings['volume_size']
    name = inst_settings['volume_name']
    mount_point = inst_settings['mount_point']
    zone = inst_settings['zone']

    volume = conn.create_volume(size, zone)
    volume.add_tag('Name', name)
    wait_for_aws(volume, "creating")
    volume.attach(instance.id, mount_point)

    return volume

def connect_aws():
    """Cache connection to AWS
    """
    global conn
    if conn is None:
        conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    return conn

def wait_for_aws(volume, wait_on_status):
    """Poll AWS to change from wait_on_status
    """
    while volume.update() == wait_on_status:
        time.sleep(3)

get_instance_name = lambda i: None if not i.has_key('Name') else i['Name']

