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
using System.Threading;
using System.Threading.Tasks;

namespace Hadoop.Client.Jobs
{
    /// <summary>
    /// Extends an instance of Hadoop against which jobs can be submitted.
    /// </summary>
    public static class HadoopJobClientExtensions
    {
        private const int PollingInterval = 5000;
        private const int RetryCount = 5;

        /// <summary>
        /// Method that waits for a jobDetails to complete.
        /// </summary>
        /// <param name="client">The Hadoop client to use.</param>
        /// <param name="job">The jobDetails to wait for.</param>
        /// <param name="duration">The duration to wait before timing out.</param>
        /// <param name="cancellationToken">
        /// The Cancellation Token for the request.
        /// </param>
        /// <returns>An awaitable task that represents the action.</returns>
        public static async Task<JobDetails> WaitForJobCompletionAsync(
            this IHadoopJobClient client, JobCreationResults job, TimeSpan duration, CancellationToken cancellationToken)
        {
            return await client.WaitForJobCompletionAsync(job.JobId, duration, cancellationToken);
        }

        /// <summary>
        /// Method that waits for a jobDetails to complete.
        /// </summary>
        /// <param name="client">The Hadoop client to use.</param>
        /// <param name="jobId">The id of the job to wait for.</param>
        /// <param name="duration">The duration to wait before timing out.</param>
        /// <param name="cancellationToken">
        /// The Cancellation Token for the request.
        /// </param>
        /// <returns>An awaitable task that represents the action.</returns>
        public static async Task<JobDetails> WaitForJobCompletionAsync(
            this IHadoopJobClient client, string jobId, TimeSpan duration, CancellationToken cancellationToken)
        {
            var jobDetailsResults = new JobDetails {JobId = jobId, StatusCode = JobStatusCode.Unknown};

            var startTime = DateTime.UtcNow;
            var endTime = DateTime.UtcNow;

            while (ShouldRetryAgain(duration, jobDetailsResults, endTime, startTime))
            {                
                if (JobIsFinished(jobDetailsResults))
                    break;

                await TaskEx.Delay(PollingInterval, cancellationToken);
                jobDetailsResults = await GetJobWithRetry(client, jobId, cancellationToken);

                endTime = DateTime.UtcNow;
            }

            if (JobIsNotFinished(jobDetailsResults) && WaitingTimeExceded(duration, endTime, startTime))
                throw new TimeoutException("Timeout waiting for jobDetails completion");

            return jobDetailsResults;
        }

        private static bool ShouldRetryAgain(TimeSpan duration, JobDetails jobDetailsResults,
            DateTime endTime, DateTime startTime)
        {
            return jobDetailsResults != null
                   && !WaitingTimeExceded(duration, endTime, startTime)
                   && JobIsNotFinished(jobDetailsResults);
        }

        private static bool WaitingTimeExceded(TimeSpan duration, DateTime endTime, DateTime startTime)
        {
            return (endTime - startTime) >= duration;
        }

        private static bool JobIsNotFinished(JobDetails jobDetailsResults)
        {
            return jobDetailsResults.StatusCode != JobStatusCode.Completed
                   && jobDetailsResults.StatusCode != JobStatusCode.Failed
                   && jobDetailsResults.StatusCode != JobStatusCode.Canceled;
        }

        private static bool JobIsFinished(JobDetails jobDetailsResults)
        {
            return jobDetailsResults.StatusCode == JobStatusCode.Completed
                   || jobDetailsResults.StatusCode == JobStatusCode.Failed;
        }

        /// <summary>
        /// Method that waits for a jobDetails to complete.
        /// </summary>
        /// <param name="client">The Hadoop client to use.</param>
        /// <param name="job">The jobDetails to wait for.</param>
        /// <param name="duration">The duration to wait before timing out.</param>
        /// <param name="cancellationToken">
        /// The Cancellation Token for the request.
        /// </param>
        /// <returns>The jobDetails's pigJobCreateParameters.</returns>
        public static JobDetails WaitForJobCompletion(
            this IHadoopJobClient client, JobCreationResults job, TimeSpan duration, CancellationToken cancellationToken)
        {
            return WaitForJobCompletionAsync(client, job, duration, cancellationToken).Result;
        }

        internal static async Task WaitForInterval(TimeSpan interval, CancellationToken token)
        {
            var start = DateTime.Now;
            var waitFor = Math.Min((int) interval.TotalMilliseconds, 1000);
            while (DateTime.Now - start < interval)
            {
                if (token.IsCancellationRequested)
                {
                    throw new OperationCanceledException("The operation was canceled by user request.");
                }

                await TaskEx.Delay(waitFor, token);                
            }
        }

        private static async Task<JobDetails> GetJobWithRetry(IHadoopJobClient client, string jobId,
            CancellationToken cancellationToken)
        {
            JobDetails jobDetailsResults = null;

            int retryCount = 0;

            while (jobDetailsResults == null)
            {
                bool exceptionOccured;

                try
                {
                    jobDetailsResults = await client.GetJob(jobId);
                    break;
                }
                catch (Exception)
                {
                    if (retryCount >= RetryCount)
                        throw;

                    exceptionOccured = true;
                }

                if (exceptionOccured)
                {
                    await WaitForInterval(TimeSpan.FromMilliseconds(PollingInterval), cancellationToken);
                    retryCount++;
                }
            }

            return jobDetailsResults;
        }
    }
}