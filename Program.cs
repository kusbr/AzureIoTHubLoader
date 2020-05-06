using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace D2C.App
{
    class Program
    {

        static void Main(string[] args)
        {
            var fg = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("This tool is NOT recommended to be used on Production or Stage or similar environments.It will delete devices in your IoTHub");
            Console.ForegroundColor = fg;

            Console.WriteLine("Use IoTHub S3 SKU with 32 partitions to achieve higher throughput closer to the documented limits.");
            Console.WriteLine("Run this app on a high end compute optimized VM with higher IOPS.");
            Console.WriteLine("<ENTER> or CTRL+C to end the execution at any time");
            Console.WriteLine("DeviceIds created by this tool are of the format 'devN' and N starts with the load:testDeviceIdstart in appsettings.json");

            Console.WriteLine("Starting load ..");
            using (CancellationTokenSource cts = new CancellationTokenSource())
            {
                CancellationToken canTok = cts.Token;
#pragma warning disable CS4014 // //Not awaited so that the console can cancel the load <ENTER>
                new LoadD2C().InitiateLoad(canTok);
#pragma warning restore CS4014 // //Not awaited so that the console can cancel the load <ENTER>
                Console.ReadLine();
                cts.Cancel();
            }

        }
    }

    class LoadD2C
    {
        string IotHubDeviceCstrFormat = "HostName={0}.azure-devices.net;DeviceId={1};SharedAccessKey={2}";

        ConcurrentDictionary<string, string> deviceConnStrs = new ConcurrentDictionary<string, string>();
        ConcurrentDictionary<string, DeviceClient> deviceClients = new ConcurrentDictionary<string, DeviceClient>();
        CancellationToken localCancelToken;
        Microsoft.Azure.Devices.Client.Message eventMessage = null;

        public async Task InitiateLoad(CancellationToken callerCancellationToken)
        {
            using (CancellationTokenSource localCancelTokenSource = new CancellationTokenSource())
            {
                bool isLocalTokenCancelled = false;
                bool loopscompleted = false;
                try
                {
                    localCancelToken = localCancelTokenSource.Token;

                    callerCancellationToken.Register(() =>
                       {
                           //Chain caller cancellation to local cancellation
                           if (!isLocalTokenCancelled)
                               localCancelTokenSource.Cancel();
                       });

                    using (var rm = RegistryManager.CreateFromConnectionString(Config.IotHubOwnerConnectionString))
                    {
                        await DeleteDevices(rm).ConfigureAwait(false);
                        Console.WriteLine("Adding " + Config.TestDevicesCount + " new devices");
                        await AddNewDevicesAsync(rm, Config.TestDeviceIdStart, Config.TestDevicesCount).ConfigureAwait(false);
                    }

                    //Create MQTT connections for the devices in the Hub
                    InitDeviceConnections();

                    Console.WriteLine("Press ENTER (or CTRL+C) to end the load test...");

                    eventMessage = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes(Config.TestData));
                    loopscompleted = await ExecuteLoadAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }
                finally
                {
                    Cleanup(localCancelTokenSource);
                    isLocalTokenCancelled = true;
                    if (loopscompleted)
                        Console.WriteLine("Load completed");
                    else
                        Console.WriteLine("Load failed");

                }
            }
        }

        async Task<bool> ExecuteLoadAsync()
        {
            Console.WriteLine("Loading test range devices at the rate of " + Config.PerDeviceMessagesLoad + " in " + Config.Hub);
            var plresult = Parallel.ForEach(deviceConnStrs, new ParallelOptions { CancellationToken = localCancelToken }, async dev =>
               {
                   PerDeviceLoad(dev.Key);
               });
            Console.WriteLine("Completed loading of each device in " + Config.Hub);

            return plresult.IsCompleted;
        }

        bool PerDeviceLoad(string deviceId)
        {
            var plresult = Parallel.For(
                           0, Config.PerDeviceMessagesLoad,
                           new ParallelOptions { CancellationToken = localCancelToken },
                           async i =>
                           {
                                SendTestDataAsync(deviceId); 
                               Thread.Sleep(Config.ThinkTimeMilliSeconds);
                           }
                       );
            return plresult.IsCompleted;
        }

        void Cleanup(CancellationTokenSource cts)
        {
            if (!localCancelToken.IsCancellationRequested)
            {
                cts.Cancel();
            }
            foreach (var dev in deviceClients)
            {
                if (dev.Value == null) continue;
                try
                {
                    dev.Value.CloseAsync();
                }
                finally
                {
                    dev.Value.Dispose();
                }
            }
            if (eventMessage != null)
                eventMessage.Dispose();
        }

        void ConnectDevice(string deviceId)
        {
            var dc = DeviceClient.CreateFromConnectionString(deviceConnStrs[deviceId], Microsoft.Azure.Devices.Client.TransportType.Mqtt_Tcp_Only);
            deviceClients[deviceId] = dc;
            deviceClients[deviceId].OpenAsync(localCancelToken).Wait(localCancelToken); //Wait for all connections to be opened
        }

        async Task SendTestDataAsync(string deviceId)
        {
            using (var eventMsg = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes(Config.TestData)))
            {
                try
                {
                    var dc = deviceClients[deviceId];
                    await deviceClients[deviceId].SendEventAsync(eventMsg, localCancelToken).ConfigureAwait(false); //Send and forget, simple D2C
                    Console.WriteLine(string.Format("{0}: {1}: Sent message successfully", deviceId, DateTime.Now.ToLongTimeString()));
                }
                catch (ObjectDisposedException odex)
                {
                    deviceClients[deviceId] =
                        DeviceClient.CreateFromConnectionString(deviceConnStrs[deviceId], Microsoft.Azure.Devices.Client.TransportType.Mqtt_Tcp_Only);
                }
            }
        }

        async Task AddNewDevicesAsync(RegistryManager rm, int start, int number)
        {
            deviceConnStrs.Clear();
            int i = start;
            for (i = start; i < (start + number); i++)
            {
                var d = await rm.AddDeviceAsync(new Device(string.Format("dev{0}", i))).ConfigureAwait(false);
                var key = d.Authentication.SymmetricKey.PrimaryKey;
                deviceConnStrs[d.Id] = string.Format(IotHubDeviceCstrFormat, Config.Hub, d.Id, key);
                Console.Write(".");
            }
            Console.WriteLine(" ");
            Console.WriteLine("Added " + number + " new devices with deviceId starting from dev" + start + " to dev" + i + " into " + Config.Hub);

        }

        async Task DeleteDevices(RegistryManager rm)
        {
            Console.WriteLine(string.Format("Deleting existing devices with IDs in the set (dev{0} - dev{1})", Config.TestDeviceIdStart, Config.TestDeviceIdStart + Config.TestDevicesCount -1));

            int n = 0;

            StringBuilder deviceQLBuilder = new StringBuilder("select * from devices where deviceId in [");
            int noDeviceCount = 0;
            for (int i = Config.TestDeviceIdStart; i < (Config.TestDeviceIdStart + Config.TestDevicesCount); i++)
            {
                try
                {
                    await rm.RemoveDeviceAsync("dev" + i.ToString(), localCancelToken);
                }
                catch(Microsoft.Azure.Devices.Common.Exceptions.DeviceNotFoundException dnfe)
                {
                    noDeviceCount++;
                }
            }

            Console.WriteLine(noDeviceCount + " deviceIds in the range not found in " + Config.Hub);
            Console.WriteLine("Deleted existing " + n + " devices from " + Config.Hub);
        }

        void InitDeviceConnections()
        {
            Console.WriteLine("Initiating device connections to IoT Hub");
            Parallel.ForEach(
                deviceConnStrs,
                new ParallelOptions { CancellationToken = localCancelToken },
                dev =>
                {
                     ConnectDevice(dev.Key);
                });
           Console.WriteLine(String.Format("Connected {0} devices to {1} using MQTT over TCP", Config.TestDevicesCount, Config.Hub));
        }

    }

    class Config
    {
        private static IConfigurationRoot jsonConfiguration;

        public static string Hub => jsonConfiguration["iothub:name"];
        public static string IoTHubOwnerAccessKey => jsonConfiguration["iothub:ownerProfileAccessKey"];
        public static string TestData => jsonConfiguration["load:testData"];
        public static int TestDevicesCount => int.Parse(jsonConfiguration["load:testDevicesCount"]);
        public static int PerDeviceMessagesLoad => int.Parse(jsonConfiguration["load:messagesPerDevice"]);
        public static int TestDeviceIdStart => int.Parse(jsonConfiguration["load:testDeviceIdstart"]);
        public static int ThinkTimeMilliSeconds => int.Parse(jsonConfiguration["load:thinkTimeMilliSecondsPerDevice"]);
        public static string IotHubOwnerConnectionString => string.Format("HostName={0}.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey={1}", Hub, IoTHubOwnerAccessKey);

        static Config()
        {
            ServiceCollection serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            serviceCollection.AddLogging();

            // Build configuration
            jsonConfiguration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetParent(AppContext.BaseDirectory).FullName)
                .AddJsonFile("appsettings.json", false)
                .Build();

            // Add access to generic IConfigurationRoot
            serviceCollection.AddSingleton<IConfigurationRoot>(jsonConfiguration);

        }
    }
}
