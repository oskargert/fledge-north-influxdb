
"""Influxdb North plugin"""
import asyncio
import json

# from fledge.common import logger
# from fledge.plugins.north.common.common import *

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError

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
	influxdb_north = InfludDBNorthPlugin(data)
	config = data
	_LOGGER.info("Initialized north influxdb plugin")
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
		_LOGGER.info("received data to insert.")
		is_data_sent, new_last_object_id, num_sent = await influxdb_north.send_payloads(payload)
	except asyncio.CancelledError:
		_LOGGER.exception("Data could not be sent. Error: {}.".format(error))
		pass
	else:
		return is_data_sent, new_last_object_id, num_sent

def plugin_shutdown():
	influxdb_north.close_session()

class InfludDBNorthPlugin(object):

	def __init__(self, settings: dict):
		url = settings["host"]["value"] + ":" + settings["port"]["value"]
		url = "http://" + url if "http" not in url else url
		self._client = InfluxDBClient(url=url, token=settings["token"]["value"], org=settings["org"]["value"])
		self._settings = settings

	async def send_payloads(self, payloads):
		is_data_sent = False
		last_object_id = 0
		num_sent = 0
		try:
			payload_block = list(map(lambda datapoint: {
														"measurement": self._settings["measurement"]["value"], 
														"fields": datapoint["reading"], 
														"tags": {
														    "asset_code": datapoint["asset_code"]
														    }, 
														"time": datapoint["user_ts"]
													}, 
                                                    payloads))
			num_sent = await self._send_payloads(payload_block)
			is_data_sent = True
			last_object_id = payloads[-1]["id"]
		except Exception as error:
			_LOGGER.exception("Data could not be sent. Error: {}.".format(error))

		_LOGGER.info("is_data_sent: {}, last_object_id: {}, num_sent: {}.".format(is_data_sent, last_object_id, num_sent))
		return is_data_sent, last_object_id, num_sent

	async def _send_payloads(self, payload_block):
		try:
			write_api = self._client.write_api(write_options=ASYNCHRONOUS)
			write_api.write(bucket=self._settings["bucket"]["value"], org=self._settings["org"]["value"], record=payload_block)
			_LOGGER.info("inserted data in bucket: {}.".format(self._settings["bucket"]["value"]))
		except Exception as error:
			# if error.response.status == 401:
			# 	_LOGGER.exception("Insufficient write permissions to {}.".format(self._settings["bucket"]["value"]))
			# else:
			_LOGGER.exception("Unable to send payload to bucket: {}. Error: {}.".format(self._settings["bucket"]["value"], error))
			return 0

		else:
			return len(payload_block)

	def close_session():
		self._client.close()