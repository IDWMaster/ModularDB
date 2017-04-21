using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{
    public static class Algorithms
    {
        public unsafe static ulong Hash(byte* ptr, int length)
        {
            ulong hash = 0;
            for (int i = 0; i < length; i++)
            {
                ulong value = ptr[i];
                hash += value >> i;
            }
            return hash;
        }
        /// <summary>
        /// Generates a lexographic hash of this byte array
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
