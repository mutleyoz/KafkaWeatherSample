using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Infrastructure
{
    public class KafkaBase<T>
    {
        public virtual void Listen(CancellationTokenSource cts, Action<T> consumeEvent)
        {
            throw new ArgumentException("Cannot perform Listen commands on clients configured as Producers");
        }

        public virtual void Send(string topic, string key, T message)
        {
            throw new ArgumentException("Cannot perform Send operations on clients configured as Consumers");
        }
    }
}
