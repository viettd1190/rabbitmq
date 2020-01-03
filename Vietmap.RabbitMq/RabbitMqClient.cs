#region

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NLog;
using RabbitMQ.Client;

#endregion

namespace Vietmap.RabbitMq
{
    public class RabbitMqClient
    {

        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public RabbitMqClient()
        {
            Uri = string.Empty;
        }

        public RabbitMqClient(string uri) : this()
        {
            Uri = uri;
            Tag = string.Empty;
        }

        public RabbitMqClient(string userName, string passWord, string hostName, string virtualHost, int port, string tag) : this(userName,
                                                                                                                                  passWord,
                                                                                                                                  hostName,
                                                                                                                                  virtualHost,
                                                                                                                                  port,
                                                                                                                                  true,
                                                                                                                                  tag)
        {
        }

        public RabbitMqClient(string userName, string passWord, string hostName, string virtualHost, int port, bool noAck, string tag) : this()
        {
            Channel = null;
            Connection = null;
            BaseConsumer = null;

            // factory info
            Username = userName;
            Password = passWord;
            HostName = hostName;
            VirtualHost = virtualHost;
            Port = port;
            IsConnected = true;
            Index = int.MaxValue;
            Terminal = false;
            NoAck = noAck;
            Tag = tag;
        }

        public string Uri { get; set; }

        public bool NoAck { get; set; }

        public bool Terminal { get; set; }

        public int Index { get; set; }

        public string Username { get; set; }

        public string Password { get; set; }

        public string HostName { get; set; }

        public string VirtualHost { get; set; }

        public int Port { get; set; }

        public string QueueName { get; set; }

        private IModel Channel { get; set; }

        public IBaseConsumer BaseConsumer { get; set; }

        private IConnection Connection { get; set; }

        public bool IsConnected { get; set; }

        public string FileName { get; private set; }

        public bool IsRestoredPublishDataFile { get; private set; } = true;

        public string Tag { get; set; }

        public void Publish(ConnectionFactory factory, string fileName)
        {
            try
            {
                Connection = factory.CreateConnection();

                Channel = Connection.CreateModel();

                // event
                Connection.ConnectionShutdown += Connection_ConnectionShutdown;
                Channel.ModelShutdown += Channel_ModelShutdown;

                FileName = fileName;
                if (!string.IsNullOrEmpty(fileName) && IsRestoredPublishDataFile)
                {
                    RestorePublishDataFromCachedFile();
                }
            }
            catch (Exception exception)
            {
                IsConnected = false;
                Logger.Debug(exception);
            }
        }

        private void Channel_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            IsConnected = false;
            if (!string.IsNullOrEmpty(QueueName))
            {
                Logger.Debug("ModelShutdown {0} {1} {2}",
                             HostName,
                             QueueName,
                             DateTime.Now);
                Logger.Debug("{0} {1}",
                             e.ReplyCode,
                             e.ReplyText);
            }
        }

        private void Connection_ConnectionShutdown(object sender, ShutdownEventArgs reason)
        {
            IsConnected = false;
            if (!string.IsNullOrEmpty(QueueName))
            {
                Logger.Debug("ConnectionShutdown {0} {1} {2}",
                             HostName,
                             QueueName,
                             DateTime.Now);
            }
        }

        public void Close()
        {
            Terminal = true;
            if (BaseConsumer != null)
            {
                BaseConsumer.Terminal = true;
            }
            SavePublishDataToCachedFile();
            CloseRabbitMqConnection();
            IsConnected = false;
        }

        private void CloseRabbitMqConnection()
        {
            try
            {
                Channel?.Close();
            }
            catch (Exception exception)
            {
                Logger.Debug(exception);
            }
            try
            {
                Connection?.Close();
            }
            catch (Exception exception)
            {
                Logger.Debug(exception);
            }
        }

        #region QueueBind

        public void QueueBind(string queue, string exchange, string routingKey)
        {
            if (IsConnected == false)
            {
                RecoveryDisconnectedPublisher();
            }
            if (IsConnected)
            {
                Channel.QueueBind(queue,
                                  exchange,
                                  routingKey);
            }
        }

        #endregion

        #region Pulisher

        private List<PublishRaw> _raws;

        public List<PublishRaw> GetRaws()
        {
            return _raws ?? (_raws = new List<PublishRaw>());
        }

        public void Publish(string exchange, string key, string msg, bool retry = true)
        {
            Publish(exchange,
                    key,
                    Encoding.UTF8.GetBytes(msg),
                    retry);
        }

        public void Publish(string exchange, string key, byte[] bytes, bool retry = true)
        {
            Publish(exchange,
                    key,
                    bytes,
                    retry,
                    true);
        }

        private void Publish(string exchange, string key, byte[] bytes, bool retry, bool resend)
        {
            if (Terminal)
            {
                return;
            }

            if (IsConnected == false)
            {
                RecoveryDisconnectedPublisher();
            }
            bool fail = true;
            if (IsConnected)
            {
                try
                {
                    // Channel.ConfirmSelect();
                    Channel.BasicPublish(exchange,
                                         key,
                                         null,
                                         bytes);

                    // Channel.TxCommit();
                    // Channel.ConfirmSelect();
                    // Channel.WaitForConfirmsOrDie();

                    fail = false;
                }
                catch (Exception exception)
                {
                    Logger.Info("ERROR {0} {1} {2}",
                                exchange,
                                key,
                                Tag);
                    Logger.Debug(exception);
                }
            }
            if (fail)
            {
                if (retry)
                {
                    GetRaws()
                            .Add(new PublishRaw
                                 {
                                         E = exchange,
                                         K = key,
                                         D = bytes,
                                         I = GetRaws()
                                                 .Count
                                 });
                }
            }
            else if (resend)
            {
                if (GetRaws()
                        .Any())
                {
                    List<PublishRaw> raws = GetRaws();
                    _raws = null;
                    foreach (PublishRaw raw in raws.OrderBy(c => c.I))
                    {
                        Publish(raw.E,
                                raw.K,
                                raw.D,
                                true,
                                false);
                    }
                }
            }
        }

        #endregion

        #region Consumer

        public void Consumer<T>(ConnectionFactory factory, string queueName) where T : IBaseConsumer
        {
            Consumer<T>(factory,
                        queueName,
                        NoAck);
        }

        public void Consumer<T>(ConnectionFactory factory, string queueName, bool noAck) where T : IBaseConsumer
        {
            IBaseConsumer basicConsumer = Activator.CreateInstance<T>();
            Consumer(factory,
                     queueName,
                     basicConsumer,
                     noAck);
        }

        public void Consumer<T>(ConnectionFactory factory, string queueName, T consume) where T : IBaseConsumer
        {
            Consumer(factory,
                     queueName,
                     consume,
                     NoAck);
        }

        public void Consumer<T>(ConnectionFactory factory, string queueName, T consume, bool noAck) where T : IBaseConsumer
        {
            try
            {
                QueueName = queueName;
                BaseConsumer = consume;
                Publish(factory,
                        string.Empty);
                NoAck = noAck;
                BaseConsumer handle = consume as BaseConsumer;
                if (handle != null)
                {
                    handle.Consumer = Channel;
                    handle.NoAck = noAck;
                }
                Channel.BasicConsume(queueName,
                                     noAck,
                                     consume);
                Logger.Info("QUEUE {0} connected! {1}",
                            queueName,
                            DateTime.Now);
            }
            catch (Exception exception)
            {
                IsConnected = false;
                Logger.Debug(exception);
            }
        }

        #endregion

        #region Recovery

        public void RecoveryDisconnectedConsumer()
        {
            if (Terminal == false)
            {
                try
                {
                    CloseRabbitMqConnection();

                    IsConnected = true;
                    if (string.IsNullOrEmpty(Uri))
                    {
                        Publish(new ConnectionFactory
                                {
                                        UserName = Username,
                                        Password = Password,
                                        VirtualHost = VirtualHost,
                                        HostName = HostName,
                                        Port = Port
                                },
                                FileName);
                    }
                    else
                    {
                        Publish(new ConnectionFactory
                                {
                                        Uri = new Uri(Uri)
                                },
                                FileName);
                    }
                    BaseConsumer handle = BaseConsumer as BaseConsumer;
                    if (handle != null)
                    {
                        handle.Consumer = Channel;
                        handle.NoAck = NoAck;
                    }
                    Channel.BasicConsume(QueueName,
                                         NoAck,
                                         BaseConsumer);
                }
                catch (Exception exception)
                {
                    IsConnected = false;
                    Logger.Info("CONSUMER {0} {1} {2}",
                                Uri,
                                HostName,
                                QueueName);
                    Logger.Info(exception);
                }
            }
        }

        public void RecoveryDisconnectedPublisher()
        {
            if (Terminal == false)
            {
                try
                {
                    CloseRabbitMqConnection();

                    IsConnected = true;
                    if (string.IsNullOrEmpty(Uri))
                    {
                        Publish(new ConnectionFactory
                                {
                                        UserName = Username,
                                        Password = Password,
                                        VirtualHost = VirtualHost,
                                        HostName = HostName,
                                        Port = Port
                                },
                                FileName);
                    }
                    else
                    {
                        Publish(new ConnectionFactory
                                {
                                        Uri = new Uri(Uri)
                                },
                                FileName);
                    }
                }
                catch (Exception exception)
                {
                    IsConnected = false;
                    Logger.Info("PUBLISHER {0} {1}",
                                Uri,
                                HostName);
                    Logger.Info(exception);
                }
            }
        }

        #endregion

        #region File IO for Publisher

        private void SavePublishDataToCachedFile()
        {
            if (!string.IsNullOrEmpty(FileName))
            {
                try
                {
                    List<PublishRaw> raws = GetRaws();
                    _raws = null;
                    AppUtil.SaveCachedDataToFile(raws,
                                                 FileName);
                }
                catch (Exception exception)
                {
                    Logger.Debug(exception);
                }
            }
        }

        private void RestorePublishDataFromCachedFile()
        {
            IsRestoredPublishDataFile = false;
            if (File.Exists(FileName))
            {
                try
                {
                    List<PublishRaw> raws = AppUtil.RestoreCachedDataFromFile<PublishRaw>(FileName);
                    if (raws?.Any() == true)
                    {
                        foreach (PublishRaw raw in raws)
                        {
                            if (raw?.D?.Length > 0)
                            {
                                GetRaws()
                                        .AddRange(raws);
                            }
                        }
                    }
                }
                catch (Exception exception)
                {
                    Logger.Debug(exception);
                }
            }
        }

        #endregion
    }
}