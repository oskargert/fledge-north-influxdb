
"""Influxdb North plugin"""
import asyncio
import json
from datetime import datetime

from fledge.common import logger
from fledge.plugins.north.common.common import *

from InfluxDBNorthPlugin import InfluxDBNorthPlugin

__author__ = "Oskar Gert"
__copyright__ = "Copyright (c) 2023 Oskar Gert"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

import logging
_LOGGER = logging
# _LOGGER = logger.setup(__name__)

_DEFAULT_CONFIG = {
		"plugin": {
			"description": "Send readings to a Influxdb server",
			"type": "string",
			"default": "influxdb_python",
			"readonly": "true"
			},
		"host": {
			"description": "The hostname of the InfluxDB server",
			"type": "string",
			"default": "http://localhost",
			"order": "1",
			"displayName": "Host"
			},
		"port": { 
			"description": "The port for this InfluxDB server",
			"type": "integer",
			"default": "8086",
			"order": "2",
			"displayName": "Port"
			},
		"bucket": {
			"description": "The bucket to insert data into",
			"type": "string",
			"default": "fledge",
			"order": "3",
			"displayName": "Bucket"
			},
		"measurement": {
			"description": "Measurement to write the data to",
			"type": "string",
			"default": "fledge",
			"order": "4",
			"displayName": "Measurement"
			},
		"org": {
			"description": "Influxdb Org",
			"type": "string",
			"default": "fledge",
			"order": "5",
			"displayName": "Organisation"
			},
		"token": {
			"description": "Access token for the database",
			"type": "string",
			"default": "",
			"order": "6",
			"displayName": "Token"
			},
		"username": {
			"description": "The InfluxDB database user name",
			"type": "string",
			"default": "",
			"order": "7",
			"displayName": "Username"
			},
		"password": {
			"description": "The InfluxDB database user password",
			"type": "password",
			"default": "",
			"order": "8",
			"displayName": "Password"
			},
		"source": {
			"description": "Defines the source of the data to be sent on the stream",
			"type": "enumeration",
			"default": "readings",
			"options": ["readings", "statistics"],
		       	"order": "9",
			"displayName": "Source"
			}
	}

def plugin_info():
	return {
		'name': 'influxdb_north_python',
		'version': '${VERSION}',
		'type': 'north',
		'interface': '1.0',
		'config': _DEFAULT_CONFIG
	}

def plugin_init(data):
	global influxdb_north, config
	influxdb_north = InfluxDBNorthPlugin(data, _LOGGER)
	config = data
	print("Initialized north influxdb plugin")
	return config

async def plugin_send(data, payload, stream_id):
	""" Used to send the readings block from north to the configured destination.

	Args:
		handle - An object which is returned by plugin_init
		payload - A List of readings block
		stream_id - An Integer that uniquely identifies the connection from Fledge instance to the destination system
	Returns:
		Tuple which consists of
		- A Boolean that indicates if any data has been sent
		- The object id of the last reading which has been sent
		- Total number of readings which has been sent to the configured destination

	Example payload:
		[
			{
				"reading": {
					"sinusoid": 0.0
				},
				"asset_code": "sinusoid",
				"id": 1,
				"ts": "2021-09-27 06: 55: 52.692000+00: 00",
				"user_ts": "2021-09-27 06: 55: 49.947058+00: 00"
			},
			{
				"reading": {
					"sinusoid": 0.104528463
				},
				"asset_code": "sinusoid",
				"id": 2,
				"ts": "2021-09-27 06: 55: 52.692000+00: 00",
				"user_ts": "2021-09-27 06: 55: 50.947110+00: 00"
			}
		]	
	
	"""
	try:
		print("received data to insert.")
		is_data_sent, new_last_object_id, num_sent = await influxdb_north.send_payloads(payload)
	except asyncio.CancelledError:
		_LOGGER.exception("Data could not be sent. Error: {}.".format(error))
		pass
	else:
		return is_data_sent, new_last_object_id, num_sent

def plugin_shutdown():
	pass

