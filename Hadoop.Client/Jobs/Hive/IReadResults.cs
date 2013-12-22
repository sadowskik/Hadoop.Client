using System.Collections.Generic;

namespace Hadoop.Client.Jobs.Hive
{
    public interface IReadResults<out TResult>
    {
        IEnumerable<TResult> Deserialize(string queryResult);
    }
}