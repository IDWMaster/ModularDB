using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{

    public class ScalableMessage
    {
        /// <summary>
        /// The message ID
        /// </summary>
        public Guid ID;
        /// <summary>
        /// Sender of the message
        /// </summary>
        public Guid From;
        /// <summary>
        /// The message
        /// </summary>
        public byte[] Message;
        public string Topic;
    }

    /// <summary>
    /// Scalable message queue
    /// Each message in this queue will be delivered to all members.
    /// A message; once received shall be processed by each node,
    /// and then acknowledged after each node has had a chance to act on the message.
    /// A posted message can either succeed (completely) or fail for all members.
    /// If it fails on a particular server, that server should be shut down and removed from the network.
    /// </summary>
    public abstract class ScalableQueue:IDisposable
    {
        public event Action<Guid> onPeerConnected;
        public event Action<ScalableMessage> onMessageReceived;

        protected void NtfyPeer(Guid peer)
        {
            onPeerConnected?.Invoke(peer);
        }
        /// <summary>
        /// Notifies the cluster that a message has been successfully processed.
        /// </summary>
        /// <param name="message"></param>
        public abstract Task CompleteMessage(ScalableMessage message);
        protected void NtfyMessage(ScalableMessage message)
        {
            if (message.From == ID)
            {
                CompleteMessage(message);
            }
            else
            {
                onMessageReceived?.Invoke(message);
            }
        }

        /// <summary>
        /// (Atomic primitive) Sends a raw message to all members of the queue.
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public abstract Task SendMessage(ScalableMessage msg);

        /// <summary>
        /// (Atomic primitive) Sends a (raw) message to all members of the queue interested in the topic
        /// </summary>
        /// <param name="topic">The topic to send on</param>
        /// <param name="msg">The message to send</param>
        /// <returns></returns>
        public Task SendMessage(string topic, byte[] msg)
        {
            return SendMessage(new ScalableMessage() { From = ID, ID = Guid.NewGuid(), Message = msg, Topic = topic });
        }

        /// <summary>
        /// (Eventually consistent) Subscribes to a topic. This is NOT an atomic operation.
        /// </summary>
        /// <param name="topic">The topic to subscribe to</param>
        /// <returns></returns>
        public abstract Task Subscribe(string topic);

        /// <summary>
        /// (Eventually consistent) Unsubscribes from a topic. This is NOT an atomic operation, and does not guarantee that messages from this topic will be blocked immediately.
        /// </summary>
        /// <param name="topic">The topic to unsubscribe from</param>
        /// <returns></returns>
        public abstract Task Unsubscribe(string topic);

        /// <summary>
        /// Returns the unique ID of this computer.
        /// The ID MAY be persisted to disk, and restored for each connection
        /// to this queue, but is not guaranteed to be persisted.
        /// </summary>
        public abstract Guid ID { get; }

        #region IDisposable Support
        protected bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {

                disposedValue = true;
            }
        }
        

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
            
        }
        #endregion
    }
}
