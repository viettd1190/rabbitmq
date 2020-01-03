using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using NLog;
using RabbitMQ.Client;

namespace Vietmap.RabbitMq
{
    public class BaseConsumer
    {

        protected static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public IModel Consumer { get; internal set; }

        public bool NoAck { get; set; } = true;

        public bool Terminal { get; set; } = false;

        public virtual string GetServiceCachedFolder()
        {
            return "";
        }

        public virtual string GetServiceCachedName()
        {
            return "";
        }

        protected void BasicAck(ulong deliveryTag)
        {
            if (!NoAck &&
                Terminal == false)
            {
                try
                {
                    Consumer?.BasicAck(deliveryTag,
                                       false);
                }
                catch (Exception exception)
                {
                    Logger.Info(exception);
                }
            }
        }

        protected void BasicReject(ulong deliveryTag)
        {
            try
            {
                Consumer?.BasicReject(deliveryTag,
                                      true);
            }
            catch (Exception exception)
            {
                Logger.Info(exception);
            }
        }

        #region I/O

        public void SaveCachedDataToFile<T>(IEnumerable<T> values)
        {
            AppUtil.SaveCachedDataToFile(values,
                                         Path.Combine(GetServiceCachedFolder(),
                                                      GetServiceCachedName()));
        }

        public List<T> RestoreCachedDataFromFile<T>()
        {
            return AppUtil.RestoreCachedDataFromFile<T>(Path.Combine(GetServiceCachedFolder(),
                                                                     GetServiceCachedName()));
        }

        #endregion

        #region I/O

        public void SaveCachedData<T>(IEnumerable<T> values)
        {
            Logger.Debug("Save to cached file: {0}", GetServiceCachedName());
            try
            {
                string cachedFile = Path.Combine(GetServiceCachedFolder(), GetServiceCachedName());
                File.WriteAllText(cachedFile, JsonConvert.SerializeObject(values, Formatting.None));
            }
            catch (Exception exception)
            {
                Logger.Debug("Save failed");
                Logger.Debug(exception);
            }
        }

        public List<T> RestoreCachedData<T>()
        {
            List<T> statuses = new List<T>();
            try
            {
                string cachedFile = Path.Combine(GetServiceCachedFolder(), GetServiceCachedName());
                if (File.Exists(cachedFile))
                {
                    StreamReader reader = new StreamReader(cachedFile, Encoding.UTF8);
                    JsonReader jsonReader = new JsonTextReader(reader);
                    JsonSerializer se = new JsonSerializer();
                    T[] obj = se.Deserialize<T[]>(jsonReader);
                    if (obj != null)
                    {
                        if (obj.Length > 0)
                        {
                            foreach (T item in obj)
                            {
                                statuses.Add(item);
                            }
                        }
                    }
                    reader.Close();
                }
            }
            catch (Exception exception)
            {
                Logger.Debug("Restore failed for cached file: {0}", GetServiceCachedName());
                Logger.Debug(exception);
            }
            Logger.Debug("Restore from cached file: {0}", GetServiceCachedName());
            return statuses;
        }

        #endregion
    }
}