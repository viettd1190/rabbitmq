#region

using System;
using System.Collections.Generic;
using System.Linq;
using System.Timers;
using NLog;
using RabbitMQ.Client;

#endregion

namespace Vietmap.RabbitMq
{
    public class RabbitMqManager
    {

        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private int _indexPublisher = 1000000;

        public RabbitMqManager()
        {
            Clients = new List<RabbitMqClient>();

            #region Initialize timer

            _globalTimer = new Timer
                           {
                               Interval = 1000
                           };
            _globalTimer.Elapsed += GlobalTimer_Elapsed;
            _globalTimer.Start();

            #endregion
        }

        public List<RabbitMqClient> Clients { get; }

        public RabbitMqClient Consumer<T>(ConnectionFactory factory, string queueName, string tag) where T : IBaseConsumer
        {
            RabbitMqClient client = new RabbitMqClient(factory.UserName, factory.Password, factory.HostName, factory.VirtualHost, factory.Port, tag);
            Clients.Add(client);
            client.Index = Clients.Count;
            client.Consumer<T>(factory, queueName);
            return client;
        }

        public RabbitMqClient Consumer<T>(ConnectionFactory factory, string queueName, T consumer, string tag) where T : IBaseConsumer
        {
            RabbitMqClient client = new RabbitMqClient(factory.UserName, factory.Password, factory.HostName, factory.VirtualHost, factory.Port, tag);
            Clients.Add(client);
            client.Index = Clients.Count;
            client.Consumer(factory, queueName, consumer);
            return client;
        }

        public RabbitMqClient Consumer<T>(ConnectionFactory factory, string queueName, bool noAck, string tag) where T : IBaseConsumer
        {
            RabbitMqClient client = new RabbitMqClient(factory.UserName, factory.Password, factory.HostName, factory.VirtualHost, factory.Port, tag);
            Clients.Add(client);
            client.Index = Clients.Count;
            client.Consumer<T>(factory, queueName, noAck);
            return client;
        }

        public RabbitMqClient Consumer<T>(ConnectionFactory factory, string queueName, T consumer, bool noAck, string tag) where T : IBaseConsumer
        {
            RabbitMqClient client = new RabbitMqClient(factory.UserName, factory.Password, factory.HostName, factory.VirtualHost, factory.Port, tag);
            Clients.Add(client);
            client.Index = Clients.Count;
            client.Consumer(factory, queueName, consumer, noAck);
            return client;
        }

        public RabbitMqClient Publish(ConnectionFactory factory, string tag, string fileName = "")
        {
            return Publish(factory, _indexPublisher++, tag, fileName);
        }

        public RabbitMqClient Publish(ConnectionFactory factory, int index, string tag, string fileName = "")
        {
            RabbitMqClient client = new RabbitMqClient(factory.UserName, factory.Password, factory.HostName, factory.VirtualHost, factory.Port, tag);
            Clients.Add(client);
            client.Index = index;
            client.Publish(factory, fileName);

            return client;
        }

        public RabbitMqClient Publish(string uri, string cached = "")
        {
            return Publish(uri, _indexPublisher++, cached);
        }

        public RabbitMqClient Publish(string uri, int index, string cached = "")
        {
            RabbitMqClient client = new RabbitMqClient(uri);
            Clients.Add(client);
            client.Index = index;
            client.Publish(new ConnectionFactory
                           {
                               Uri = new Uri(uri)
                           }, cached);

            return client;
        }

        public void Close()
        {
            StopGlobalTimer();
            foreach (RabbitMqClient client in Clients.OrderBy(c => c.Index))
            {
                try
                {
                    Logger.Info("Closing connection to queue {0} {1}", client.HostName, client.QueueName);
                    client.Close();
                    Logger.Info("Closed");
                }
                catch (Exception exception)
                {
                    Logger.Info(exception);
                }
            }
        }

        #region Global Timer

        private readonly Timer _globalTimer;

        private int _counter;

        public void StopGlobalTimer()
        {
            if (_globalTimer != null)
            {
                _globalTimer.Stop();
            }
        }

        private void GlobalTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            _counter++;

            if (_counter > 60)
            {
                _counter = 0;
                RecoveryDisconnectedConsumer();
            }
        }

        private void RecoveryDisconnectedConsumer()
        {
            List<RabbitMqClient> list = Clients.Where(c => c.IsConnected == false && c.BaseConsumer != null).ToList();
            if (list.Any())
            {
                Logger.Debug("RabbitMqManager Recovery Disconnected Consumer {0}", DateTime.Now);
                foreach (RabbitMqClient client in list)
                {
                    client.RecoveryDisconnectedConsumer();
                }
            }
        }

        #endregion

    }
}