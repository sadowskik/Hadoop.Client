using System.Collections.Generic;

namespace Hadoop.Client.Jobs
{    
    public abstract class JobCreateParameters
    {
        /// <summary>
        /// Initializes a new instance of the JobCreateParameters class.
        /// </summary>
        protected JobCreateParameters()
        {
            Files = new List<string>();
            EnableTaskLogs = false;
        }

        /// <summary>
        /// Gets the resources for the jobDetails.
        /// </summary>
        public ICollection<string> Files { get; private set; }

        /// <summary>
        /// Gets or sets the status folder to use for the jobDetails.
        /// </summary>
        public string StatusFolder { get; set; }

        /// <summary>
        /// Gets or sets the callback URI to be called upon job completion.
        /// </summary>
        public string Callback { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the job executor should persist task logs.
        /// </summary>
        public bool EnableTaskLogs { get; set; }
    }
}
