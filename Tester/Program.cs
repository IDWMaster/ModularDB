using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureDB;
using System.Diagnostics;
using System.Net;

namespace Tester
{
    class Program
    {
        static IEnumerable<ScalableEntity> GenerateEntities()
        {
            byte[] value = new byte[1024];
            Random mrand = new Random();
            while(true)
            {
                yield return new ScalableEntity(Guid.NewGuid().ToByteArray(), value);
            }
        }
        static void clustertests(ScalableDb db)
        {
            Console.Clear();
            Console.WriteLine();
            Console.WriteLine("Connecting to bus");
            Console.Write("Please enter a valid host:port: ");
            string[] ep = Console.ReadLine().Split(':');


            using (P2PQueue q = new P2PQueue(ep[0], int.Parse(ep[1])))
            {
                Console.WriteLine("Connecting to bus (topic test)");
                var subtask = q.Subscribe("test");
                subtask.Wait();
                if (subtask.Exception != null)
                {
                    throw subtask.Exception;
                }
                Console.WriteLine("We're on the bus!");
                q.onMessageReceived += async msg =>
                {
                    Console.WriteLine(msg.From + ":" + Encoding.UTF8.GetString(msg.Message));
                    await q.CompleteMessage(msg);
                };
                q.onPeerConnected += peer =>
                {
                    Console.WriteLine("Peer connected: " + peer);
                };
                while (true)
                {
                    var tsk = q.SendMessage("test", Encoding.UTF8.GetBytes(Console.ReadLine()));
                    tsk.Wait();
                    if (tsk.Exception != null)
                    {
                        throw tsk.Exception;
                    }
                }
            }
        }

        
        static async Task prog_main()
        {
            using (ScalableDb db = new ScaledAzureDb(File.ReadAllLines("C:\\data\\config.txt").Select(m=>new AzureDatabase(m,"testabletables")).ToList().ToArray()))
            {
                var oldcolor = Console.BackgroundColor;
                Console.BackgroundColor = ConsoleColor.Red;
                Console.WriteLine("WARNING: These tests may make destructive changes to your database. Never use this test suite on a production database.");
                Console.BackgroundColor = oldcolor;
                Console.WriteLine("Please select a test suite:");
                Console.WriteLine("0. Database connectivity and cluster performance test");
                Console.WriteLine("1. Writer connectivity test");
                Console.Write("Please enter a selection: ");
                switch (Console.ReadKey().KeyChar)
                {
                    case '1':
                        //Sanity test
                        clustertests(db);
                        return;
                }
                Console.Clear();

                ScalableDb mdb = db;
                //mdb = new MemoryDb();
                List<ScalableEntity> ents = new List<ScalableEntity>();
                Stopwatch pwatch = new Stopwatch();
                pwatch.Start();
                await db.Retrieve(null, null, rows => {
                    lock(ents)
                    {
                        ents.AddRange(rows);
                    }
                    return true;
                });
                pwatch.Stop();
                Console.WriteLine("Loaded " + ents.Count + " in " + pwatch.Elapsed);
                ents = null;
                while (true)
                {
                    Console.WriteLine("Enter number of entities to test");
                    int count = int.Parse(Console.ReadLine());
                    var entities = GenerateEntities().Take(count).ToList();
                    Stopwatch mwatch = new Stopwatch();
                    mwatch.Start();
                    await mdb.Upsert(entities);
                    mwatch.Stop();
                    Console.WriteLine("Upsert took " + mwatch.Elapsed);
                    mwatch = new Stopwatch();
                    var retrieves = entities.Select(m => m.Key).ToList();
                    entities.Clear();
                    mwatch.Start();
                    await mdb.Retrieve(retrieves, data => {
                        lock (entities)
                        {
                            entities.AddRange(data);
                        }
                        return true;
                    });
                    mwatch.Stop();
                    Console.WriteLine("Retrieved " + entities.Count + " in " + mwatch.Elapsed);
                }
            }
            
        }
        static void Main(string[] args)
        {
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.DefaultConnectionLimit = 100;
            prog_main().Wait();
        }
    }
}
