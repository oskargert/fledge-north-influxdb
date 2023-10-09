import logging
from datetime import datetime, timezone
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async  import InfluxDBClientAsync
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.domain.write_precision import WritePrecision

class InfluxDBNorthPlugin(object):

	def __init__(self, settings: dict, logger = None):
		url = settings["host"]["value"] + ":" + settings["port"]["value"]
		self._url = "http://" + url if "http" not in url else url
		self._settings = settings
		self._LOGGER = logging.getLogger(__name__) if logger is None else logger
		self._LOGGER.info("Initilized InfluxDBNorthPlugin")

	async def send_payloads(self, payloads):
		is_data_sent = False
		last_object_id = 0
		num_sent = 0
		try:
			payload_block = list(map(lambda datapoint: 
				Point(datapoint["asset_code"])
					.tag("plugin", 
						"fledge"
					)
					.field(
						list(datapoint["reading"].keys())[0], 
						list(datapoint["reading"].values())[0]
					)
					.time(
						datapoint["user_ts"]
					),
					payloads))
			num_sent = await self._send_payloads(payload_block)
			is_data_sent = True
			last_object_id = payloads[-1]["id"]
		except Exception as error:
			self._LOGGER.exception("Data could not be sent. Error: {}.".format(error))

		self._LOGGER.info("is_data_sent: {}, last_object_id: {}, num_sent: {}.".format(is_data_sent, last_object_id, num_sent))
		return is_data_sent, last_object_id, num_sent

	async def _send_payloads(self, payload_block):
		try:
			async with InfluxDBClientAsync(url=self._url, token=self._settings["token"]["value"], org=self._settings["org"]["value"], debug=False) as client:
				write_api = client.write_api()
				write_msg = await write_api.write(bucket=self._settings["bucket"]["value"], org=self._settings["org"]["value"], record=payload_block)
		except Exception as error:
			self._LOGGER.exception("Unable to send payload to bucket: {}. Error: {}.".format(self._settings["bucket"]["value"], error))
			return 0

		else:
			self._LOGGER.info("Inserted data in bucket: {}.".format(self._settings["bucket"]["value"]))
			return len(payload_block)
