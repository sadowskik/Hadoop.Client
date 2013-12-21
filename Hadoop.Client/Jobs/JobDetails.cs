// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

using System;

namespace Hadoop.Client.Jobs
{
    /// <summary>
    /// Represents the job results of an HadoopJob.
    /// </summary>
    public class JobDetails : JobCreationResults
    {
        /// <summary>
        /// Initializes a new instance of the JobDetails class.
        /// </summary>
        public JobDetails()
        {
            JobId = "unknown";
            ErrorOutputPath = string.Empty;
            ExitCode = -1;
            JobId = string.Empty;
            LogicalOutputPath = string.Empty;
            Name = string.Empty;
            PhysicalOutputPath = string.Empty;
            Query = string.Empty;
            StatusCode = JobStatusCode.Unknown;
            StatusDirectory = string.Empty;
            SubmissionTime = DateTime.MinValue;
        }

        /// <summary>
        /// Gets or sets the exit code for the job.
        /// </summary>
        public int? ExitCode { get; set; }

        /// <summary>
        /// Gets or sets the status directory for the job.
        /// </summary>
        public string StatusDirectory { get; set; }

        /// <summary>
        /// Gets or sets the name of the job.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the query for the job (if it was a hive job).
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// Gets or sets the status code for the job.
        /// </summary>
        public JobStatusCode StatusCode { get; set; }

        /// <summary>
        /// Gets or sets the time the job was submitted.
        /// </summary>
        public DateTime SubmissionTime { get; set; }

        /// <summary>
        /// Gets or sets the error output path for the job.
        /// </summary>
        public string ErrorOutputPath { get; set; }

        /// <summary>
        /// Gets or sets the logical output path for the job results.
        /// </summary>
        public string LogicalOutputPath { get; set; }

        /// <summary>
        /// Gets or sets the physical output path for the job results.
        /// </summary>
        public string PhysicalOutputPath { get; set; }

        /// <summary>
        /// Gets or sets the percentage completion of the job.
        /// </summary>
        public string PercentComplete { get; set; }

        /// <summary>
        /// Gets or sets the uri to call when this job completes.
        /// </summary>
        public string Callback { get; set; }
    }
}
