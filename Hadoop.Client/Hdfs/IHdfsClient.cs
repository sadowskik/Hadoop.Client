using System.IO;
using System.Threading.Tasks;

namespace Hadoop.Client.Hdfs
{
    public interface IHdfsClient
    {
        Task<Stream> OpenFile(string path);

        Task<string> CreateFile(string path, Stream content, bool overwrite);

        Task<bool> CreateDirectory(string path);

        Task<bool> Delete(string path, bool recursive);

        Task<DirectoryEntry> GetFileStatus(string path);        
    }
}
