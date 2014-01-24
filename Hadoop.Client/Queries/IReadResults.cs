using System.Collections.Generic;

namespace Hadoop.Client.Queries
{
    public interface IReadResults<out TResult>
    {
        IEnumerable<TResult> Deserialize(string queryResult);
    }
}