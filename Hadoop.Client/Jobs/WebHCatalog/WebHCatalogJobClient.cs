using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace Hadoop.Client.Jobs.WebHCatalog
{
    public class WebHCatalogJobClient : IHadoopJobClient
    {
        private readonly ConnectionConfig _connectionConfig;

        public WebHCatalogJobClient(ConnectionConfig connectionConfig)
        {
            _connectionConfig = connectionConfig;}

        public async Task<JobList> ListJobs()
        {
            var relative =
                new Uri(
                    HadoopRemoteRestConstants.Jobs + "?" + HadoopRemoteRestConstants.UserName + "=" + _connectionConfig.UserName.EscapeDataString()
                    + "&" + HadoopRemoteRestConstants.ShowAllFields,
                    UriKind.Relative);

            return await MakeAsyncGetRequest(relative);
        }

        public async Task<JobDetails> GetJob(string jobId)
        {
            var relative =
                new Uri(
                    HadoopRemoteRestConstants.Jobs + "/" + jobId.EscapeDataString() + "?" + HadoopRemoteRestConstants.UserName + "=" +
                    _connectionConfig.UserName.EscapeDataString(),
                    UriKind.Relative);
            return await MakeAsyncGetRequest(relative);
        }

        public async Task<JobCreationResults> SubmitMapReduceJob(MapReduceJobCreateParameters details)
        {
            var relative = new Uri(HadoopRemoteRestConstants.MapReduceJar + "?" +
                                       HadoopRemoteRestConstants.UserName + "=" +
                                       _connectionConfig.UserName.EscapeDataString(),
                                       UriKind.Relative);
            return await MakeAsyncJobSubmissionRequest(relative, payload);
        }

        public async Task<JobCreationResults> SubmitHiveJob(HiveJobCreateParameters details)
        {
            var relative = new Uri(HadoopRemoteRestConstants.Hive + "?" +
                                       HadoopRemoteRestConstants.UserName + "=" +
                                       _connectionConfig.UserName.EscapeDataString(),
                                       UriKind.Relative);
            return await MakeAsyncJobSubmissionRequest(relative, payload);
        }

        public async Task<JobCreationResults> SubmitPigJob(PigJobCreateParameters pigJobCreateParameters)
        {
            var relative = new Uri(HadoopRemoteRestConstants.MapReduceStreaming + "?" +
                                       HadoopRemoteRestConstants.UserName + "=" +
                                       _connectionConfig.UserName.EscapeDataString(),
                                       UriKind.Relative);
            return await MakeAsyncJobSubmissionRequest(relative, payload);
        }

        public async Task<JobCreationResults> SubmitSqoopJob(SqoopJobCreateParameters sqoopJobCreateParameters)
        {
            var relative = new Uri(HadoopRemoteRestConstants.Pig + "?" +
                                        HadoopRemoteRestConstants.UserName + "=" +
                                        _connectionConfig.UserName.EscapeDataString(),
                                        UriKind.Relative);
            return await MakeAsyncJobSubmissionRequest(relative, payload);
        }

        public async Task<JobCreationResults> SubmitStreamingJob(StreamingMapReduceJobCreateParameters pigJobCreateParameters)
        {
            var relative = new Uri(HadoopRemoteRestConstants.Sqoop + "?" +
                                         HadoopRemoteRestConstants.UserName + "=" +
                                         _connectionConfig.UserName.EscapeDataString(),
                                         UriKind.Relative);
            return await MakeAsyncJobSubmissionRequest(relative, payload);
        }

        public async Task<JobDetails> StopJob(string jobId)
        {
            var relative = new Uri(HadoopRemoteRestConstants.Jobs + "/" +
                                        jobId.EscapeDataString() + "?" +
                                        HadoopRemoteRestConstants.UserName + "=" +
                                        _connectionConfig.UserName.EscapeDataString(),
                                        UriKind.Relative);

            return await this.MakeAsyncJobCancellationRequest(relative);
        }
        
        private async Task<HttpResponseMessage> MakeAsyncGetRequest(Uri relativeUri)
        {
            var client = CreateHttpClient();

            var uri = new Uri(_connectionConfig.Server, relativeUri);
            var response = await client.GetAsync(uri);

            response.EnsureSuccessStatusCode();
            return response;
        }

        private async Task<HttpResponseMessage> MakeAsyncJobSubmissionRequest(Uri relativeUri, string payload)
        {
            var client = CreateHttpClient();

            var uri = new Uri(_connectionConfig.Server, relativeUri);
            var response = await client.PostAsync(uri, new StringContent(payload));

            response.EnsureSuccessStatusCode();
            return response;
        }

        private async Task<HttpResponseMessage> MakeAsyncJobCancellationRequest(Uri relativeUri)
        {
            var client = CreateHttpClient();

            var uri = new Uri(_connectionConfig.Server, relativeUri);
            var response = await client.DeleteAsync(uri);

            response.EnsureSuccessStatusCode();
            return response;
        }

        private HttpClient CreateHttpClient(bool allowsAutoRedirect = true)
        {
            return HttpClientBuilder.Create(allowsAutoRedirect)
                .WithBasicAuthenticationFrom(_connectionConfig)
                .AcceptJson()
                .AcceptOctetStream()
                .Build();
        }
    }
}