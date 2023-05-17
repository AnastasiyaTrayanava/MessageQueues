using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MessageQueues.Common.Models
{
    [Serializable]
    public class FileProperties
    {
        public string Name { get; set; }
        public byte[] Body { get; set; }
        public int ChunkNumber { get; set; }
        public int TotalChunks { get; set; } 
    }
}
