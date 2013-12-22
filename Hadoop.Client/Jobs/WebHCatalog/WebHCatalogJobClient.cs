using System;
using System.Net.Http;
using System.Threading.Tasks;
using Hadoop.Client.Jobs.WebHCatalog.Data;

namespace Hadoop.Client.Jobs.WebHCatalog
{
    public class WebHCatalogJobClient : IHadoopJobClient
    {
        private readonly ConnectionConfig _connectionConfig;
        private readonly IPayloadConverter _converter;

        public WebHCatalogJobClient(ConnectionConfig connectionConfig)
        {
            _connectionConfig = connectionConfig;
            _converter = new JsonPayloadConverter();
        }

        public async Task<JobList> ListJobs()
        {
            var relative =
                new Uri(
                    HadoopRemoteRestConstants.Jobs + "?" + HadoopRemoteRestConstants.UserName + "=" + _connectionConfig.UserName.EscapeDataString()
                    + "&" + HadoopRemoteRestConstants.ShowAllFields,
                    UriKind.Relative);

            var result = await MakeAsyncGetRequest(relative);
            return _converter.DeserializeListJobResult(result);
        }

        public async Task<JobDetails> GetJob(string jobId)
        {
            var relative =
                new Uri(
                    HadoopRemoteRestConstants.Jobs + "/" + jobId.EscapeDataString() + "?" + HadoopRemoteRestConstants.UserName + "=" +
                    _connectionConfig.UserName.EscapeDataString(),
                    UriKind.Relative);

            var result = await MakeAsyncGetRequest(relative);            
            return _converter.DeserializeJobDetails(result);
        }

        public async Task<JobCreationResults> SubmitMapReduceJob(MapReduceJobCreateParameters details)
        {
            var relative = new Uri(
                HadoopRemoteRestConstants.MapReduceJar + "?" +
                HadoopRemoteRestConstants.UserName + "=" +
                _connectionConfig.UserName.EscapeDataString(),
                UriKind.Relative);

            var requestContent = _converter.SerializeMapReduceRequest(_connectionConfig.UserName, details);
            var result = await MakeAsyncJobSubmissionRequest(relative, requestContent);
            
            return new JobCreationResults { JobId = _converter.DeserializeJobSubmissionResponse(result) };
        }

        public async Task<JobCreationResults> SubmitHiveJob(HiveJobCreateParameters details)
        {
            var relative = new Uri(
                HadoopRemoteRestConstants.Hive + "?" +
                HadoopRemoteRestConstants.UserName + "=" +
                _connectionConfig.UserName.EscapeDataString(),
                UriKind.Relative);

            var requestContent = _converter.SerializeHiveRequest(_connectionConfig.UserName, details);
            var result = await MakeAsyncJobSubmissionRequest(relative, requestContent);
            
            return new JobCreationResults { JobId = _converter.DeserializeJobSubmissionResponse(result) };
        }

        public async Task<JobCreationResults> SubmitPigJob(PigJobCreateParameters pigJobCreateParameters)
        {
            var relative = new Uri(
                HadoopRemoteRestConstants.MapReduceStreaming + "?" +
                HadoopRemoteRestConstants.UserName + "=" +
                _connectionConfig.UserName.EscapeDataString(),
                UriKind.Relative);
            
            var requestContent = _converter.SerializePigRequest(_connectionConfig.UserName, pigJobCreateParameters);
            var result = await MakeAsyncJobSubmissionRequest(relative, requestContent);
            
            return new JobCreationResults {JobId = _converter.DeserializeJobSubmissionResponse(result)};
        }

        public async Task<JobCreationResults> SubmitSqoopJob(SqoopJobCreateParameters sqoopJobCreateParameters)
        {
            var relative = new Uri(
                HadoopRemoteRestConstants.Pig + "?" +
                HadoopRemoteRestConstants.UserName + "=" +
                _connectionConfig.UserName.EscapeDataString(),
                UriKind.Relative);

            var requestContent = _converter.SerializeSqoopRequest(_connectionConfig.UserName, sqoopJobCreateParameters);
            var result = await MakeAsyncJobSubmissionRequest(relative, requestContent);
            
            return new JobCreationResults { JobId = _converter.DeserializeJobSubmissionResponse(result) };
        }

        public async Task<JobCreationResults> SubmitStreamingJob(StreamingMapReduceJobCreateParameters pigJobCreateParameters)
        {
            var relative = new Uri(
                HadoopRemoteRestConstants.Sqoop + "?" +
                HadoopRemoteRestConstants.UserName + "=" +
                _connectionConfig.UserName.EscapeDataString(),
                UriKind.Relative);

            var requestContent = _converter.SerializeStreamingMapReduceRequest(_connectionConfig.UserName, pigJobCreateParameters);
            var result = await MakeAsyncJobSubmissionRequest(relative, requestContent);
            
            return new JobCreationResults { JobId = _converter.DeserializeJobSubmissionResponse(result) };
        }

        public async Task<JobDetails> StopJob(string jobId)
        {
            var relative = new Uri(
                HadoopRemoteRestConstants.Jobs + "/" +
                jobId.EscapeDataString() + "?" +
                HadoopRemoteRestConstants.UserName + "=" +
                _connectionConfig.UserName.EscapeDataString(),
                UriKind.Relative);

            var result = await MakeAsyncJobCancellationRequest(relative);
            return _converter.DeserializeJobDetails(result);
        }
        
        private async Task<string> MakeAsyncGetRequest(Uri relativeUri)
        {
            var client = CreateHttpClient();

            var uri = new Uri(_connectionConfig.Server, relativeUri);
            var response = await client.GetAsync(uri);

            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        private async Task<string> MakeAsyncJobSubmissionRequest(Uri relativeUri, string payload)
        {
            var client = CreateHttpClient();

            var uri = new Uri(_connectionConfig.Server, relativeUri);
            var response = await client.PostAsync(uri, new StringContent(payload));

            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        private async Task<string> MakeAsyncJobCancellationRequest(Uri relativeUri)
        {
            var client = CreateHttpClient();

            var uri = new Uri(_connectionConfig.Server, relativeUri);
            var response = await client.DeleteAsync(uri);

            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
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