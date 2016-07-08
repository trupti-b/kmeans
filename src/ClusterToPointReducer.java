import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.io.IOException;

/**
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class ClusterToPointReducer extends
		Reducer<IntWritable, Point, IntWritable, Point> {

	public void reduce(IntWritable key, Iterable<Point> values, Context context)
			throws IOException, InterruptedException {

		int centroidIndex = key.get();
		int numberOfPoints = 0;
		Float[] sum = new Float[KMeans.dimension];
		for(int i = 0; i < KMeans.dimension; i++){
			sum[i] = (float) 0.0;
		}
		for (Point p : values) {
			System.out.println(p.coordinates);
			numberOfPoints++;
			for (int i = 0; i < KMeans.dimension; i++) {
				sum[i] = sum[i] + p.coordinates.get(i);
			}

		}

		Point newCentroid = new Point(KMeans.dimension);
		for (int i = 0; i < KMeans.dimension; i++) {
			newCentroid.coordinates.add(sum[i] * (1/(float) numberOfPoints));
			//newCentroid.coordinates.add(sum[i] / (float) numberOfPoints);
		}

		KMeans.centroids.set(centroidIndex, newCentroid);
	}
}
