using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Net.Sockets;
namespace AzureDB
{

    /// <summary>
    /// P2P decentralized queue
    /// The database will store a list of all members of the queue
    /// If an attempt to contact a node fails; the node shall contact all other members of the queue to see if those nodes can contact the affected node.
    /// This can be used to detect network partitions. If any other server is able to contact the affected node; the system will keep trying to connect
    /// until no other nodes are able to reach the affected host.
    /// If no nodes are able to reach the affected host; the system shall remove
    /// the affected node from the database, marking it as offline.
    /// </summary>
    class P2PQueue : ScalableQueue
    {
        UdpClient mclient;
        Task initTask;
        public P2PQueue(byte[] name, TableDb db, string hostname, int port, TimeSpan timeout)
        {
            initTask = Initialize(name,db,hostname,port,timeout);
        }
        byte[] name;
        byte[] entry;
        TableDb db;
        bool running = true;
        Dictionary<IPEndPoint, TaskCompletionSource<bool>> pendingPings = new Dictionary<IPEndPoint, TaskCompletionSource<bool>>();
        Dictionary<Guid, TaskCompletionSource<bool>> pendingPingRequests = new Dictionary<Guid, TaskCompletionSource<bool>>();
        async Task<bool> Ping(IPEndPoint ep)
        {
            TaskCompletionSource<bool> a = new TaskCompletionSource<bool>();
            lock(pendingPings)
            {
                pendingPings.Add(ep, a); //EPA! EEEPAAA!
            }
            await mclient.SendAsync(new byte[] { 0 }, 1,ep);

            await Task.WhenAny(Task.Delay(timeout),a.Task);
            lock(pendingPings)
            {
                pendingPings.Remove(ep);
            }
            return a.Task.IsCompleted;
            
        }
        TimeSpan timeout;
        async Task Initialize(byte[] name, TableDb db, string hostname, int port, TimeSpan timeout)
        {
            this.timeout = timeout;
            this.name = name;
            this.db = db;
            entry = new byte[name.Length + 16];
            Buffer.BlockCopy(name, 0, entry, 0, name.Length);
            Buffer.BlockCopy(id.ToByteArray(), 0, entry, name.Length, 16);

            var row = TableRow.From(new { Key = entry, Hostname = hostname, port = port });
            row.UseLinearHash = true;

            mclient = new UdpClient(new IPEndPoint(IPAddress.Any, port));
            Action recvLoop = async () => {
                while(running)
                {
                    try
                    {
                        var packet = await mclient.ReceiveAsync(); //Receive a sink.
                        BinaryReader mreader = new BinaryReader(new MemoryStream(packet.Buffer));
                        switch(mreader.ReadByte())
                        {
                            case 0:
                                //Ping request
                                await mclient.SendAsync(new byte[] { 1 }, 1, packet.RemoteEndPoint);
                                break;
                            case 1:
                                //Ping response
                                TaskCompletionSource<bool> msrc = null;
                                lock(pendingPings)
                                {
                                    if(pendingPings.ContainsKey(packet.RemoteEndPoint))
                                    {
                                        msrc = pendingPings[packet.RemoteEndPoint];
                                        pendingPings.Remove(packet.RemoteEndPoint);
                                    }
                                }
                                msrc?.SetResult(true);
                                break;
                            case 2:
                                //Ping endpoint request
                                if(await Ping(new IPEndPoint(new IPAddress(mreader.ReadBytes(4)),mreader.ReadInt32())))
                                {
                                    byte[] me = new byte[17];
                                    me[0] = 3;
                                    Buffer.BlockCopy(mreader.ReadBytes(16), 0, me, 1, 16);
                                    await mclient.SendAsync(me, me.Length,packet.RemoteEndPoint);
                                }
                                break;
                            case 3:
                                //Ping endpoint response
                                msrc = null;
                                Guid ian = new Guid(mreader.ReadBytes(16));
                                lock (pendingPingRequests)
                                {

                                    if(pendingPingRequests.ContainsKey(ian))
                                    {
                                        msrc = pendingPingRequests[ian];
                                        pendingPingRequests.Remove(ian);
                                    }
                                }
                                msrc?.SetResult(true);
                                break;
                        }
                    }catch(Exception er)
                    {

                    }
                }
            };

            await db["__queues"].Upsert(row);
        }




        Guid id = Guid.NewGuid();
        public override Guid ID => id;

        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                db["__queues"].Delete(new byte[][] { entry }).ContinueWith(tsk => {
                    running = false;
                    mclient.Close();
                });
            }
            base.Dispose(disposing);
        }

        public override async Task CompleteMessage(ScalableMessage message)
        {
            throw new NotImplementedException();
        }
        

        public override async Task SendMessage(ScalableMessage msg)
        {
            await initTask;
          //  await db["__queues"].Retrieve()

            throw new NotImplementedException();
        }
    }
}
