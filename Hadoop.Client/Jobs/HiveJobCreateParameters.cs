using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Hadoop.Client.Jobs
{    
    public sealed class HiveJobCreateParameters : JobCreateParameters
    {        
        public HiveJobCreateParameters()
        {
            Defines = new Dictionary<string, string>();
            Arguments = new Collection<string>();
        }

        /// <summary>
        /// Gets or sets the name of the jobDetails.
        /// </summary>
        public string JobName { get; set; }

        /// <summary>
        /// Gets or sets the query to use for a hive jobDetails.
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// Gets the parameters for the jobDetails.
        /// </summary>
        public IDictionary<string, string> Defines { get; private set; }

        /// <summary>
        /// Gets the arguments for the jobDetails.
        /// </summary>
        public ICollection<string> Arguments { get; private set; }

        /// <summary>
        /// Gets or sets the query file to use for a hive jobDetails.
        /// </summary>
        public string File { get; set; }
    }
}
