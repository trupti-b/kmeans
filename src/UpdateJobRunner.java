import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class UpdateJobRunner {
	/**
	 * Create a map-reduce job to update the current centroids.
	 * 
	 * @precondition The global centroids variable has been set.
	 */
	public static Job createUpdateJob(int jobId, String inputDirectory,
			String outputDirectory) throws IOException {
		Job update_job = new Job(new Configuration(), "kmeans_2");
		update_job.setJarByClass(KMeans.class);
		update_job.setMapperClass(PointToClusterMapper.class);
		update_job.setMapOutputKeyClass(IntWritable.class);
		update_job.setMapOutputValueClass(Point.class);
		update_job.setReducerClass(ClusterToPointReducer.class);
		update_job.setOutputKeyClass(IntWritable.class);
		update_job.setOutputValueClass(Point.class);
		FileInputFormat.addInputPath(update_job, new Path(inputDirectory));
		FileOutputFormat.setOutputPath(update_job, new Path(outputDirectory));
		update_job.setInputFormatClass(KeyValueTextInputFormat.class);
		return update_job;
	}

	/**
	 * Run the jobs until the centroids stop changing. Let C_old and C_new be
	 * the set of old and new centroids respectively. We consider C_new to be
	 * unchanged from C_old if for every centroid, c, in C_new, the L2-distance
	 * to the centroid c' in c_old is less than [epsilon].
	 *
	 * Note that you may retrieve publically accessible variables from other
	 * classes by prepending the name of the class to the variable (e.g.
	 * KMeans.one).
	 *
	 * @param maxIterations
	 *            The maximum number of updates we should execute before we stop
	 *            the program. You may assume maxIterations is positive.
	 * @param inputDirectory
	 *            The path to the directory from which to read the files of
	 *            Points
	 * @param outputDirectory
	 *            The path to the directory to which to put Hadoop output files
	 * @return The number of iterations that were executed.
	 */
	public static int runUpdateJobs(int maxIterations, String inputDirectory,
			String outputDirectory) throws IOException, InterruptedException,
			ClassNotFoundException {
		int count = 0;
		while (count < maxIterations) {
			count++;
			
			ArrayList<Point> old_centroids = new ArrayList<Point>(
					KMeans.centroids);

			Job updateJob = createUpdateJob(count, inputDirectory,
					outputDirectory + "/kmeans" + count);
			System.out.println("created update job");
			updateJob.waitForCompletion(true);
			ArrayList<Point> new_centroids = new ArrayList<Point>(
					KMeans.centroids);
			int j = 0;
			for(j = 0; j < old_centroids.size(); j++){
				Point old_Point = old_centroids.get(j);
				Point new_Point = new_centroids.get(j);
				float distance = Point.distance(old_Point, new_Point);
				if(distance >= 0.000001f){
					break;
				}
			}
			if(j == (old_centroids.size())){
				//You have iterated through all centroids without breaking out
				break;
			}
			
		}

		return count;
	}
}
