namespace Hadoop.Client.Jobs.WebHCatalog
{
    public interface IPayloadConverter : IPayloadSeriazlier, IPlayloadDeserializer
    {
    }

    public interface IPayloadSeriazlier
    {
        string SerializeStreamingMapReduceRequest(string userName, StreamingMapReduceJobCreateParameters pigJobCreateParameters);

        string SerializeSqoopRequest(string userName, SqoopJobCreateParameters sqoopJobCreateParameters);

        string SerializePigRequest(string userName, PigJobCreateParameters pigJobCreateParameters);

        string SerializeHiveRequest(string userName, HiveJobCreateParameters details);

        string SerializeMapReduceRequest(string userName, MapReduceJobCreateParameters details);
    }

    public interface IPlayloadDeserializer
    {
        JobList DeserializeListJobResult(string result);

        JobDetails DeserializeJobDetails(string result);

        string DeserializeJobSubmissionResponse(string result);
    }
}