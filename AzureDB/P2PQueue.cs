using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
namespace AzureDB
{
    class PeerInformation
    {
        public IPEndPoint ep;
        public Dictionary<string, int> subscriptions = new Dictionary<string, int>();
        public void Start(int bytesSent)
        {
            this.bytesSent = bytesSent;
            initialTimer.Start();
        }
        public void Stop()
        {
            initialTimer.Stop();
        }
        public TimeSpan GetTimeoutInterval(int bytes)
        {
            long tval = initialTimer.ElapsedMilliseconds;
            if(tval == 0)
            {
                tval = 4;
            }
            tval *= 2; //Round-trip time
            if(bytes<bytesSent)
            {
                bytes = bytesSent+1;
            }
            var retval = TimeSpan.FromTicks((bytes-bytesSent)*(initialTimer.ElapsedTicks*2));
            return retval;
        }
        int bytesSent;
        System.Diagnostics.Stopwatch initialTimer = new System.Diagnostics.Stopwatch();
    }

    /// <summary>
    /// P2P decentralized queue via UDP
    /// </summary>
    public class P2PQueue : ScalableQueue
    {

        UdpClient mclient;
        Dictionary<Guid, PeerInformation> peers = new Dictionary<Guid, PeerInformation>();
        HashSet<string> subscriptions = new HashSet<string>();
        object workevt_sync = new object();
        TaskCompletionSource<bool> workevt = new TaskCompletionSource<bool>();
        object work_sync = new object();
        Queue<Func<Task>> work = new Queue<Func<Task>>();

        void QueueWork(Func<Task> function)
        {
            lock(work_sync)
            {
                work.Enqueue(function);
            }
            lock(workevt_sync)
            {
                workevt.SetResult(true);
            }
        }
        Task initTask;
        bool running = true;
        async Task UpdateSubscriptions(Guid peer)
        {

        }
        public P2PQueue(string hostname, int port)
        {
            mclient = new UdpClient(AddressFamily.InterNetworkV6);
            mclient.Client.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.IPv6Only, false);
            try
            {
                mclient.Client.Bind(new IPEndPoint(IPAddress.IPv6Any, port));
            }catch(Exception er)
            {
                mclient.Client.Bind(new IPEndPoint(IPAddress.IPv6Any, 0));
            }
            TaskCompletionSource<bool> source = new TaskCompletionSource<bool>();
            initTask = source.Task;
            Action item = async () => {
                try
                {
                    Queue<UdpReceiveResult> pendingPackets = new Queue<UdpReceiveResult>();
                    byte[] regPacket = new byte[1 + 16];
                    Buffer.BlockCopy(id.ToByteArray(), 0, regPacket, 1, 16);
                    while (running)
                    {
                        await mclient.SendAsync(regPacket, regPacket.Length, hostname, port);
                        Task<UdpReceiveResult> recvTask = mclient.ReceiveAsync();
                        if (await Task.WhenAny(Task.Delay(1000), recvTask) == recvTask)
                        {
                            MemoryStream mstream = new MemoryStream(recvTask.Result.Buffer);
                            BinaryReader mreader = new BinaryReader(mstream);
                            switch (mreader.ReadByte())
                            {
                                case 0:
                                    if (new Guid(mreader.ReadBytes(16)) == ID)
                                    {
                                        source.SetResult(true);
                                        goto velociraptor;
                                    }else
                                    {
                                        pendingPackets.Enqueue(recvTask.Result);
                                    }
                                    break;
                                case 1:
                                    {
                                        await UpdatePeerlist(recvTask.Result, mreader);
                                        source.SetResult(true);
                                        goto velociraptor;
                                    }
                                default:
                                    pendingPackets.Enqueue(recvTask.Result);
                                    break;
                            }
                        }
                    }
                    velociraptor:
                    while(running)
                    {
                        //Contains 1 msg
                        UdpReceiveResult msgs;
                        if (pendingPackets.Any())
                        {
                            msgs = pendingPackets.Dequeue();
                        }
                        else
                        {
                           var recvTask = mclient.ReceiveAsync();
                            dengo:
                            var result = Task.WhenAny(recvTask, workevt.Task);
                            if(result.Result == workevt.Task)
                            {
                                lock(workevt_sync)
                                {
                                    workevt = new TaskCompletionSource<bool>();
                                }
                                Queue<Func<Task>> work = null;
                                lock(work_sync)
                                {
                                    work = this.work;
                                    this.work = new Queue<Func<Task>>();
                                }
                                //Perform work within this context
                                foreach(var iable in work)
                                {
                                    await iable();
                                }
                                goto dengo;
                            }
                            msgs = recvTask.Result;
                        }
                        BinaryReader mreader = new BinaryReader(new MemoryStream(msgs.Buffer));
                        switch(mreader.ReadByte())
                        {
                            case 0:
                                {
                                    //New peer found
                                    Guid ian = new Guid(mreader.ReadBytes(16));
                                    if(ian == id)
                                    {
                                        break;
                                    }
                                    if(peers.ContainsKey(ian))
                                    {
                                        break;
                                    }
                                    peers[ian] = new PeerInformation() { ep = msgs.RemoteEndPoint };
                                    NtfyPeer(ian);
                                    //Reply with peerlist and update subscriptions (NOTE: In-order delivery is NOT guaranteed here)
                                    byte[] me = new byte[1 + 16+((16+8+2)*peers.Count)];
                                    me[0] = 1;
                                    Buffer.BlockCopy(ID.ToByteArray(), 0, me, 1, 16);
                                    int i = 0;
                                    foreach (var iable in peers)
                                    {
                                        var baseoffset = (1 + 16 + ((16 + 8 + 2) * i));
                                        Buffer.BlockCopy(iable.Key.ToByteArray(), 0, me, baseoffset, 16);
                                        Buffer.BlockCopy(iable.Value.ep.Address.GetAddressBytes(), 0, me, baseoffset + 16, 8);
                                        Buffer.BlockCopy(BitConverter.GetBytes((ushort)iable.Value.ep.Port), 0, me, baseoffset + 16+8, 2);

                                        i++;
                                    }
                                    await mclient.SendAsync(me, me.Length, msgs.RemoteEndPoint);
                                    await UpdateSubscriptions(ian);
                                }
                                break;
                            case 1:
                                {
                                    await UpdatePeerlist(msgs, mreader);
                                }
                                break;
                        }
                    }
                }catch(Exception er)
                {

                }
            };
            item();
        }

        private async Task UpdatePeerlist(UdpReceiveResult data, BinaryReader mreader)
        {
            //New peerlist
            Guid ian = new Guid(mreader.ReadBytes(16));
            if (ian != ID)
            {
                if(!peers.ContainsKey(ian))
                {
                    peers[ian] = new PeerInformation() { ep = data.RemoteEndPoint };
                    await UpdateSubscriptions(ian);
                    NtfyPeer(ian);
                }
            }
            while (mreader.BaseStream.Length != mreader.BaseStream.Position)
            {
                ian = new Guid(mreader.ReadBytes(16));
                var pi = new PeerInformation() { ep = new IPEndPoint(new IPAddress(mreader.ReadBytes(8)), mreader.ReadUInt16()) }; ;
                if (ian != ID && !peers.ContainsKey(ian))
                {
                    peers[ian] = pi;
                    await UpdateSubscriptions(ian);
                    NtfyPeer(ian);
                }
            }
        }

        Guid id = Guid.NewGuid();
        public override Guid ID => id;

        public override async Task CompleteMessage(ScalableMessage message)
        {
            await initTask;
            throw new NotImplementedException();
        }

        public override async Task SendMessage(ScalableMessage msg)
        {
            await initTask;
            throw new NotImplementedException();
        }
        public override async Task Subscribe(string topic)
        {
            await initTask;
            TaskCompletionSource<bool> src = new TaskCompletionSource<bool>();
            QueueWork(async() => {
                subscriptions.Add(topic);
                foreach(var iable in peers)
                {
                    //NOTE: This is to avoid flooding the network (wait for each request before moving to the next one)
                    //In a really large cluster; this is pretty important.
                    await UpdateSubscriptions(iable.Key);
                }
                src.SetResult(true);
            });
            await src.Task;
        }

        public override async Task Unsubscribe(string topic)
        {
            await initTask;
            throw new NotImplementedException();
        }
        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                running = false;
                mclient.Close();
            }
            base.Dispose(disposing);
        }
    }
}
