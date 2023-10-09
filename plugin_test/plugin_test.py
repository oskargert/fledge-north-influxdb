import json, asyncio

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'python', 'fledge', 'plugins', 'north', 'influxdb_python'))
from InfluxDBNorthPlugin import InfluxDBNorthPlugin

if __name__ == "__main__":

	with open(os.path.join('plugin_test', 'example_config.json')) as f:
		config = json.load(f)
	with open(os.path.join('plugin_test','example_data.json')) as f:
		data = json.load(f)

	influxclient = InfluxDBNorthPlugin(config)
	asyncio.run(influxclient.send_payloads(data))
	
	print("Done")