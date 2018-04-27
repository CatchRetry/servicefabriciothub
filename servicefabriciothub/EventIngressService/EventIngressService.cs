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
using TransportType = Microsoft.Azure.ServiceBus.TransportType;


namespace EventIngressService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class EventIngressService : StatefulService
    {
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

    }
}
