using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB.Tests
{
    [TestClass()]
    public class ScalableDbTests
    {
        [TestMethod()]
        public void RangeRetrieveTest()
        {
            using(MemoryDb db = new MemoryDb())
            {
                ScalableEntity[] entities = new ScalableEntity[5000];
                for(int i = 0;i<entities.Length;i++)
                {
                    entities[i] = new ScalableEntity(BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(i)), new byte[5000]) { UseLinearHash = true };
                }
                db.Upsert(entities).Wait();
                List<ScalableEntity> data = new List<ScalableEntity>();
                db.Retrieve(null, null, rows => {
                    lock(data)
                    {
                        data.AddRange(rows);
                    }
                    return true;
                }).Wait();
                Assert.AreEqual(entities.Length,data.Count);
                Assert.IsTrue(entities.Intersect(data,EntityComparer.instance).Count() == data.Count);

                data.Clear();
                int count = 0;
                object sync = new object();
                db.Retrieve(BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(200)), BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(1000)),rows=> {
                    lock(sync)
                    {
                        count += rows.Count();
                    }
                    Assert.IsFalse(rows.Where(m => {
                        int val = System.Net.IPAddress.NetworkToHostOrder(BitConverter.ToInt32(m.Key, 0));
                        return !(val > 200 && val < 1000);
                    }).Any());
                    return true;
                }).Wait();
                Assert.AreEqual(799, count);
            }
        }

        [TestMethod()]
        public void UpsertTest()
        {
            Random mrand = new Random();

            using(MemoryDb db = new MemoryDb())
            {
                ScalableEntity[] entities = new ScalableEntity[5000];
                for (int i = 0; i < entities.Length; i++)
                {
                    byte[] key = new byte[150];
                    byte[] value = new byte[1024];
                    mrand.NextBytes(value);
                    mrand.NextBytes(key);
                    entities[i] = new ScalableEntity(key,value);
                }
                db.Upsert(entities).Wait();
                List<ScalableEntity> results = new List<ScalableEntity>();
                db.Retrieve(entities.Select(m => m.Key), rows => {
                    lock(results)
                    {
                        results.AddRange(rows);
                    }
                    return true;
                }).Wait();
                Assert.AreEqual(entities.Length, results.Count);
                Assert.IsTrue(results.SequenceEqual(entities,EntityComparer.instance));
            }
        }
    }
}