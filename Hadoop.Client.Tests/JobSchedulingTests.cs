using System;
using Hadoop.Client.Hdfs.WebHdfs;
using Hadoop.Client.Jobs;
using Hadoop.Client.Jobs.WebHCatalog;
using Hadoop.Client.Queries;
using NFluent;
using Xunit;

namespace Hadoop.Client.Tests
{
    public class JobSchedulingTests
    {
        private const string WebHcatBase = @"http://sandbox.hortonworks.com:50111/";
        private const string WebHdfsBase = @"http://sandbox.hortonworks.com:50070/";

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

            Check.That(result.JobId).IsNotEmpty().And.IsNotNull();                            
            Console.WriteLine(result.JobId);
        }

        [Fact]
        public void execute_hive_query()
        {
            const string hiveQuery = @"
                SELECT s07.description, s07.total_emp, s08.total_emp, s07.salary
                    FROM
                      sample_07 s07 JOIN 
                      sample_08 s08
                    ON ( s07.code = s08.code )
                    WHERE
                    ( s07.total_emp > s08.total_emp
                     AND s07.salary > 100000 )
                    SORT BY s07.salary DESC";

            var result = CreateApacheHiveClient().Query(hiveQuery).Result;
            
            Check.That(result).IsNotEmpty().And.IsNotNull();
            Console.WriteLine(result);
        }

        private static HiveClient CreateApacheHiveClient()
        {
            var hdfsClient = new WebHdfsHttpClient(Connect.WithTestUser(WebHdfsBase));
            var hCatalogClient = new WebHCatalogJobClient(Connect.WithTestUser(WebHcatBase));

            var client = new HiveClient(hdfsClient, hCatalogClient, new HiveClientConfig
            {
                ResultsFolderBase = @"/tmp/hiveQueries/",
                StrandardOutputFileName = "stdout"
            });

            return client;
        }
    }
}