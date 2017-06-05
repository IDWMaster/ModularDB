using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;
using AzureDB;
namespace QueueServer
{
    
    class Message
    {
        public byte[] Value;
        public HashSet<Server> pendingServers = new HashSet<Server>();
        public TaskCompletionSource<bool> delivered = new TaskCompletionSource<bool>();
    }
    class Server
    {

    }
    class MessageQueue
    {
        TaskCompletionSource<Message> NewPacketAvailable = null;
        public Queue<Message> pendingMessages = new Queue<Message>();
        public HashSet<Server> Servers = new HashSet<Server>();
        public Task PostMessage(Message msg)
        {
            lock (pendingMessages)
            {
                foreach (var iable in Servers)
                {
                    msg.pendingServers.Add(iable);
                }
                if (NewPacketAvailable == null)
                {
                    pendingMessages.Enqueue(msg);
                }
                else
                {
                    NewPacketAvailable.SetResult(msg);
                    NewPacketAvailable = null;
                }

            }
            return msg.delivered.Task;

        }
        public Task<Message> GetMessage()
        {
            lock (pendingMessages)
            {
                if (NewPacketAvailable?.Task.IsCompleted == true)
                {
                    return NewPacketAvailable.Task;
                }
                if (NewPacketAvailable == null)
                {
                    NewPacketAvailable = new TaskCompletionSource<Message>();
                }
                if (pendingMessages.Any())
                {
                    NewPacketAvailable.SetResult(pendingMessages.Dequeue());
                }
                return NewPacketAvailable.Task;
            }
        }
    }


    static class Program
    {
        static async Task SendPacket(this Stream stream, byte[] packet)
        {
            MemoryStream mstream = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(mstream);
            mwriter.Write(packet.Length);
            mwriter.Write(packet);
            byte[] data = mstream.ToArray();
            await stream.WriteAsync(data, 0, data.Length);
        }
        static async Task<byte[]> ReceivePacket(this Stream stream)
        {
            byte[] buffy = new byte[4096];
            int toRead = 4;
            int offset = 0;
            while (toRead > 0)
            {
                int processed = await stream.ReadAsync(buffy, offset, toRead);
                toRead -= processed;
                offset += processed;
            }
            int len = BitConverter.ToInt32(buffy, 0);
            buffy = new byte[len];
            toRead = len;
            while (toRead > 0)
            {
                int processed = await stream.ReadAsync(buffy, offset, toRead);
                toRead -= processed;
                offset += processed;
            }
            return buffy;

        }
        static async Task prog_main(string[] args)
        {
            int portno = 3000;
            TcpListener mlist = new TcpListener(new IPEndPoint(IPAddress.Any, portno));
            Dictionary<byte[], MessageQueue> queues = new Dictionary<byte[], MessageQueue>(ByteComparer.instance);
            while(true)
            {

                MessageQueue rq = null;
                try
                {
                    var _client = await mlist.AcceptTcpClientAsync();
                    Action<TcpClient> clientThread = async client => {
                        using (client)
                        {
                            var str = client.GetStream();
                            List<Task> pendingTasks = new List<Task>();
                            HashSet<byte[]> associations = new HashSet<byte[]>();
                            Server server = new Server();
                            try
                            {
                                while (true)
                                {
                                    Task<byte[]> recvTask = str.ReceivePacket();
                                    pendingTasks.Add(recvTask);
                                    await Task.WhenAny(pendingTasks);
                                    foreach(var iable in pendingTasks)
                                    {
                                        if(iable.IsCompleted)
                                        {
                                            pendingTasks.Remove(iable);
                                        }
                                    }


                                    if(!recvTask.IsCompleted)
                                    {
                                        continue; //No new packets; continue execution.
                                    }

                                    BinaryReader mreader = new BinaryReader(new MemoryStream(recvTask.Result));
                                    byte[] q;
                                    switch (mreader.ReadByte())
                                    {
                                        case 0:
                                            //Associate with queue
                                            q = mreader.ReadBytes(mreader.ReadInt32());

                                            lock (queues)
                                            {
                                                if (!queues.ContainsKey(q))
                                                {
                                                    queues.Add(q, new MessageQueue());
                                                }

                                                rq = queues[q];
                                            }
                                            associations.Add(q);
                                            rq.Servers.Add(server);
                                            break;
                                        case 1:
                                            //Send message to queue
                                            q = mreader.ReadBytes(mreader.ReadInt32());
                                            rq = null;
                                            lock (queues)
                                            {
                                                rq = queues[q];
                                            }
                                            pendingTasks.Add(rq.PostMessage(new Message() { Value = mreader.ReadBytes(mreader.ReadInt32()) }));
                                            break;
                                    }
                                }
                            }
                            catch (Exception er)
                            {

                            }
                            finally
                            {
                                lock (queues)
                                {


                                    foreach (byte[] assoc in associations)
                                    {
                                        rq = queues[assoc];
                                        rq.Servers.Remove(server);
                                        if(!rq.Servers.Any())
                                        {
                                            queues.Remove(assoc);
                                        }
                                    }
                                }
                            }
                        }
                    };
                    clientThread(_client);
                }catch(Exception er)
                {

                }
            }
        }
        static void Main(string[] args)
        {
            prog_main(args).Wait();
        }
    }
}
