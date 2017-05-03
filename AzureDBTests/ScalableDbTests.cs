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