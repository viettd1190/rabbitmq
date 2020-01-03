using RabbitMQ.Client;

namespace Vietmap.RabbitMq
{
    public interface IBaseConsumer : IBasicConsumer
    {

        bool Terminal { get; set; }

        string GetServiceCachedName();

        void RestoreCachedData();

        void SaveCachedData();

    }
}