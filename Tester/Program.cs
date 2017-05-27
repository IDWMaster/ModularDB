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
        static async Task prog_main()
        {
            using (ScalableDb db = new ScaledAzureDb(File.ReadAllLines("C:\\data\\config.txt").Select(m=>new AzureDatabase(m,"testabletables")).ToList().ToArray()))
            {
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
