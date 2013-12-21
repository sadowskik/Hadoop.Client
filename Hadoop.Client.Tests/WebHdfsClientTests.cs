using System;
using System.IO;
using FluentAssertions;
using Hadoop.Client.Hdfs.WebHdfs;
using Xunit;

namespace Hadoop.Client.Tests
{
    public class Web_Hdfs_Client_Tests
    {
        private const string WebHdfsBase = @"http://sandbox.hortonworks.com:50070/";

        [Fact]
        public void read_content_via_web_hdfs()
        {
            var hdfsClient = new WebHdfsHttpClient(Connect.WithTestUser(to: WebHdfsBase));

            var file = hdfsClient.OpenFile("/user/hue/test.output/stdout").Result;

            using (var reader = new StreamReader(file))
            {
                var content = reader.ReadToEnd();
                Console.WriteLine();
                Console.WriteLine(content);
            }
        }

        [Fact]
        public void write_content_via_web_hdfs()
        {
            var hdfsClient = new WebHdfsHttpClient(Connect.WithTestUser(to: WebHdfsBase));

            hdfsClient.CreateDirectory("/abc2").Result.Should().BeTrue();
        }        
    }
}