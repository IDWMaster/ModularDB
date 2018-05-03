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
            // Test hash uniqueness (sorted list)
            ulong[] hashes = new ulong[255];
            int[] partitions = new int[16];
            for (int i = 0; i < 255; i++)
            {
                partitions[new byte[] { (byte)i }.Hash() % (ulong)(partitions.Length)]++;
            }
            int average = (int)partitions.Average();
            Assert.IsFalse(partitions.Where(m => Math.Abs(average - m) > partitions.Length).Any());

            //Test it with a pseudo-random dataset
            Random mrand = new Random(42);
            for (int i = 0; i < 65536; i++)
            {
                int eger = mrand.Next();
                byte* ptr = (byte*)&eger;
                partitions[Algorithms.Hash(ptr, sizeof(int)) % (ulong)partitions.Length]++;
            }
            average = (int)partitions.Average();
            Assert.IsFalse(partitions.Where(m => Math.Abs(average - m) > partitions.Length).Count() < (partitions.Length / 2));


        }

        [TestMethod()]
        public void LinearHashTest()
        {
            Assert.IsTrue(new byte[] { 1, 0 }.LinearHash() < new byte[] { 2, 0 }.LinearHash());
            Assert.IsTrue(new byte[] { 1, 0 }.LinearHash() < new byte[] { 2, 0, 255, 255, 255, 255 }.LinearHash());
            Assert.IsTrue(new byte[] { 0, 2 }.LinearHash() < new byte[] { 2, 0 }.LinearHash());
            Assert.IsTrue(new byte[] {  255, 0 }.LinearHash() > new byte[] { 0, 255 }.LinearHash());
            Assert.IsTrue(new byte[] { 2 }.LinearHash() > new byte[] { 255 }.LinearHash());
        }
    }
}