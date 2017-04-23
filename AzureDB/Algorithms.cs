using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{
    public static class Algorithms
    {
        public static ulong LinearHash(this byte[] array)
        {
            ulong forme = 0;
            int count = array.Length >= sizeof(ulong) ? sizeof(ulong) : array.Length;
            for(int i = 0;i<count;i++)
            {
                forme |= (ulong)(array[i] << ((sizeof(ulong)-i-1)*8));
            }
            
            return forme;
        }
        public static byte[] PadTo(this byte[] array,int numBytes)
        {
            byte[] retval = new byte[numBytes];
            Buffer.BlockCopy(array, 0, retval, 0, array.Length);
            return retval;
        }
        public unsafe static ulong Hash(byte* ptr, int length)
        {
            ulong hash = 14695981039346656037;
            for (int i = 0; i < length; i++)
            {
                hash = (hash ^ ptr[i]) * 1099511628211;
            }
            return hash;
        }
        /// <summary>
        /// Generates a statistically uniform hash of a byte array. The resulting hash is ideal for point queries (partitioning), but will not work for range queries.
        /// </summary>
        /// <returns></returns>
        public static unsafe ulong Hash(this byte[] data)
        {
            fixed(byte* ptr = data)
            {
                return Hash(ptr, data.Length);
            }
        }
    }
}
