using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.ServiceBus;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using TransportType = Microsoft.Azure.ServiceBus.TransportType;


namespace EventIngressService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class EventIngressService : StatefulService
    {
        private const string OffsetDictionaryName = "OffsetDictionary";

        public EventIngressService(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {

                string eventHubCompatibleEndpoint = GetEventHubCompatibleEndpoint();
                ServiceEventSource.Current.ServiceMessage(this.Context, $"Event Hub-compatible Endpoint = {eventHubCompatibleEndpoint}");
                string eventHubCompatibleName = GetEventHubCompatibleName();
                ServiceEventSource.Current.ServiceMessage(this.Context, $"Event Hub-compatible Name = {eventHubCompatibleName}");

                // Get an EventHub client connected to the IOT Hub
                var eventHubClient = GetAmqpEventHubClient(eventHubCompatibleEndpoint, eventHubCompatibleName);

                // These Reliable Dictionaries are used to keep track of our position in IoT Hub for each partition.
                // If this service fails over, this will allow it to pick up where it left off in the event stream.
                IReliableDictionary<string, string> offsetDictionary =
                    await this.StateManager.GetOrAddAsync<IReliableDictionary<string, string>>(OffsetDictionaryName);

                // Get receivers connected to the partitions of the IOT Hub
                var eventHubReceivers = await GetEventHubReceiversAsync(eventHubClient, offsetDictionary);

                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
            }
            catch (Exception e)
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "{0}", e.Message);
            }

        }

        /// <summary>
        /// Get the IoT Hub EventHub-compatible endpoint from the Settings.xml config file
        /// from a configuration package named "Config"
        /// </summary>
        /// <returns></returns>
        private string GetEventHubCompatibleEndpoint()
        {
            return this.Context.CodePackageActivationContext
                .GetConfigurationPackageObject("Config")
                .Settings
                .Sections["IoTHubConfigInformation"]
                .Parameters["EventHubCompatibleEndpoint"]
                .Value;
        }


        /// <summary>
        /// Get the IoT Hub EventHub-compatible name from the Settings.xml config file
        /// from a configuration package named "Config"
        /// </summary>
        /// <returns></returns>
        private string GetEventHubCompatibleName()
        {
            return this.Context.CodePackageActivationContext
                .GetConfigurationPackageObject("Config")
                .Settings
                .Sections["IoTHubConfigInformation"]
                .Parameters["EventHubCompatibleName"]
                .Value;
        }

        /// <summary>
        /// Creates an EventHubClient through AMQP protocol
        /// </summary>
        /// <param name="eventHubCompatibleEndpoint"></param>
        /// <param name="eventHubCompatibleName"></param>
        private EventHubClient GetAmqpEventHubClient(string eventHubCompatibleEndpoint, string eventHubCompatibleName)
        {
            // EventHubs doesn't support NetMessaging, so ensure the transport type is AMQP.
            ServiceBusConnectionStringBuilder connectionStringBuilder =
                new ServiceBusConnectionStringBuilder(eventHubCompatibleEndpoint)
                {
                    TransportType = TransportType.Amqp,
                    EntityPath = eventHubCompatibleName
                };

            ServiceEventSource.Current.ServiceMessage(
                this.Context,
                "RoutingService connecting to IoT Hub throught Amqp at {0}",
                new object[] { connectionStringBuilder.GetEntityConnectionString() });

            return EventHubClient.CreateFromConnectionString(connectionStringBuilder.GetEntityConnectionString());
        }


        /// <summary>
        /// Creates a dictionary of EventHubReceiver connected though ServiceBus and partitions
        /// with AMQP protocol
        /// </summary>
        /// <param name="eventHubClient">Event Hub Client</param>
        /// <param name="offsetDictionary"></param>
        /// <returns></returns>
        private async Task<Dictionary<string, PartitionReceiver>> GetEventHubReceiversAsync(EventHubClient eventHubClient, IReliableDictionary<string, string> offsetDictionary)
        {
            // Get the IoT Hub partitions IDs
            string[] partitionIds = (await eventHubClient.GetRuntimeInformationAsync()).PartitionIds;

            var eventHubReceivers = new Dictionary<string, PartitionReceiver>();

            foreach (var eventHubPartitionId in partitionIds)
            {
                using (ITransaction tx = this.StateManager.CreateTransaction())
                {
                    ConditionalValue<string> offsetResult =
                        await offsetDictionary.TryGetValueAsync(tx, key: eventHubPartitionId, lockMode: LockMode.Default);

                    PartitionReceiver eventHubReceiver;
                    if (offsetResult.HasValue)
                    {
                        ServiceEventSource.Current.ServiceMessage(
                            this.Context,
                            $"RoutingService connecting to IoT Hub partition ID {eventHubPartitionId} with offset {offsetResult.Value}");

                        eventHubReceiver = eventHubClient.CreateReceiver(
                            PartitionReceiver.DefaultConsumerGroupName,
                            eventHubPartitionId,
                            EventPosition.FromOffset(offsetResult.Value)
                            );
                    }
                    else
                    {
                        var dateTimeUtc = DateTime.UtcNow;
                        ServiceEventSource.Current.ServiceMessage(
                            this.Context,
                            $"RoutingService connecting to IoT Hub partition ID {eventHubPartitionId} with offset {dateTimeUtc}");

                        eventHubReceiver = eventHubClient.CreateReceiver(
                            PartitionReceiver.DefaultConsumerGroupName,
                            eventHubPartitionId,
                            EventPosition.FromEnqueuedTime(dateTimeUtc)
                        );
                    }
                    eventHubReceivers[eventHubPartitionId] = eventHubReceiver;
                }
            }
            return eventHubReceivers;
        }
    }
}
