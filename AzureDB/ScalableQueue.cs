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
    }

    /// <summary>
    /// Scalable message queue
    /// Each message in this queue will be delivered to all members.
    /// A message; once received shall be processed by each node,
    /// and then acknowledged after each node has had a chance to act on the message.
    /// A posted message can either succeed (completely) or fail for all members.
    /// If it fails on a particular server, that server should be shut down and removed from the network.
    /// </summary>
    public abstract class ScalableQueue
    {
        public event Action<ScalableMessage> onMessageReceived;

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
        /// Sends a message to all members of the queue.
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public abstract Task SendMessage(ScalableMessage msg);

        /// <summary>
        /// Returns the unique ID of this computer.
        /// The ID MAY be persisted to disk, and restored for each connection
        /// to this queue, but is not guaranteed to be persisted.
        /// </summary>
        public abstract Guid ID { get; }
    }
}
