# IoTHubLoaderApp
App to send messages to Azure IoT Hub at very high rates. Has been tested for ~257K RPM (~ 4.2K messages per second) with an S3 IoTHub 1 Throughput Unit, 2000 devices and 2000 messages/device with a thinktime of 5ms, the container was hosted on a single compute intensive high iops Azure VM. Can be deployed as a Docker container

Usage: 
Edit the appsettings.json (intuitive keys)with appropriate values.

This tool is NOT recommended to be used on Production or Stage or similar environments.It will delete devices in your IoTHub
Use IoTHub S3 SKU with 32 partitions to achieve higher throughput closer to the documented limits.
Run this app on a high end compute optimized VM with higher IOPS.
<ENTER> or CTRL+C to end the execution at any time
DeviceIds created by this tool are of the format 'devN' and N starts with the load:testDeviceIdstart in appsettings.json
Starting load ..
