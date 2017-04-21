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
    public class AlgorithmsTests
    {
        [TestMethod()]
        public unsafe void HashTest()
        {
            //Test hash ordering
            byte[] a = new byte[1];
            byte[] b = new byte[] { 1 };
            Assert.IsTrue(a.Hash() < b.Hash());
            Assert.IsTrue(new byte[] { 1, 0,0,0 }.Hash() > new byte[] { 0,0,0,1 }.Hash());
            Assert.IsTrue(new byte[] { 1, 0, 0, 0 }.Hash() > new byte[] { 0, 0, 1 }.Hash());
            Assert.IsTrue(new byte[] { 1 }.Hash() > new byte[] { 0, 0, 1 }.Hash());
            Assert.IsTrue(new byte[] { 2 }.Hash() > new byte[] { 1 }.Hash());
            Assert.IsTrue(new byte[] { 50 }.Hash() > new byte[] { 0,0,120 }.Hash());
            Assert.IsTrue(new byte[] { 120 }.Hash() > new byte[] { 0, 0, 120 }.Hash());

            // Test hash uniqueness (sorted list)
            ulong[] hashes = new ulong[255];
            int[] partitions = new int[5];
            for (int i = 0;i<255;i++)
            {
                partitions[new byte[] { (byte)i }.Hash() % 5]++;
            }
            int average = (int)partitions.Average();
            Assert.IsFalse(partitions.Where(m=>Math.Abs(average-m)>partitions.Length).Any());

            //Test it with a pseudo-random dataset
            Random mrand = new Random(5);
            for(int i = 0;i<65536;i++)
            {
                int eger = mrand.Next();
                byte* ptr = (byte*)&eger;
                partitions[Algorithms.Hash(ptr, sizeof(int)) % 5]++;
            }
            average = (int)partitions.Average();
            Assert.IsFalse(partitions.Where(m => Math.Abs(average - m) > partitions.Length).Count()<(partitions.Length/2));

        }
    }
}