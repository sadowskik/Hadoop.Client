using System.Threading.Tasks;

namespace Hadoop.Client.Jobs
{
    public interface IHadoopJobClient
    {
        Task<JobList> ListJobs();

        Task<JobDetails> GetJob(string jobId);

        Task<JobCreationResults> SubmitMapReduceJob(MapReduceJobCreateParameters details);

        Task<JobCreationResults> SubmitHiveJob(HiveJobCreateParameters details);

        Task<JobCreationResults> SubmitPigJob(PigJobCreateParameters pigJobCreateParameters);

        Task<JobCreationResults> SubmitSqoopJob(SqoopJobCreateParameters sqoopJobCreateParameters);

        Task<JobCreationResults> SubmitStreamingJob(StreamingMapReduceJobCreateParameters pigJobCreateParameters);

        Task<JobDetails> StopJob(string jobId);
    }
}
