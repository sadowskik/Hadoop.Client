using System;
using FluentAssertions;
using Hadoop.Client.Jobs;
using Hadoop.Client.Jobs.WebHCatalog;
using Xunit;

namespace Hadoop.Client.Tests
{
    public class JobSchedulingTests
    {
        private const string WebHcatBase = @"http://localhost:50111/";

        [Fact]
        public void schedule_hive_job()
        {
            var client = new WebHCatalogJobClient(Connect.WithTestUser(to: WebHcatBase));
            
            var job = new HiveJobCreateParameters
            {
                StatusFolder = "test.output",
                JobName = "test-hive-query",
                Query = @"
                    SELECT s07.description, s07.total_emp, s08.total_emp, s07.salary
                    FROM
                      sample_07 s07 JOIN 
                      sample_08 s08
                    ON ( s07.code = s08.code )
                    WHERE
                    ( s07.total_emp > s08.total_emp
                     AND s07.salary > 100000 )
                    SORT BY s07.salary DESC"
            };

            var result = client.SubmitHiveJob(job).Result;

            result.JobId.Should().NotBeNullOrEmpty();
            Console.WriteLine(result.JobId);
        }       
    }
}