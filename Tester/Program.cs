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
            using (ScaledAzureDb db = new ScaledAzureDb(File.ReadAllLines("C:\\data\\config.txt").Select(m=>new AzureDatabase(m,"testabletables")).ToList().ToArray()))
            {
                Stopwatch mwatch = new Stopwatch();
                mwatch.Start();
                await db.Upsert(GenerateEntities().Take(100000));
                mwatch.Stop();
                Console.WriteLine("Upsert took " + mwatch.Elapsed);
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
