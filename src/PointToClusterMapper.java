import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 */
public class PointToClusterMapper extends Mapper<Text, Text, IntWritable, Point>
{
	
	public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
        {
		Point p = new Point(key.toString());
		int minDistanceIndex = 0;
		Float minDistance = (float) 99999.99f;
		//for each centroid find distance, find min distance, take centroid index as key
		for(int i = 0; i < KMeans.centroids.size(); i++){
			Point centroidPoint = KMeans.centroids.get(i);
			Float distance = Point.distance(p, centroidPoint);
			if(distance < minDistance){
				minDistanceIndex = i;
				minDistance = distance;
			}
			
		}
		//System.out.println(" Centroid , Point " + KMeans.centroids.get(minDistanceIndex) + p );
		context.write(new IntWritable(minDistanceIndex), p);

        }



}
