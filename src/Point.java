import java.io.*; // DataInput/DataOuput
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.*; // Writable

/**
 * A Point is some ordered list of floats.
 * 
 * A Point implements WritableComparable so that Hadoop can serialize and send
 * Point objects across machines.
 *
 * NOTE: This implementation is NOT complete. As mentioned above, you need to
 * implement WritableComparable at minimum. Modify this class as you see fit.
 */
public class Point implements WritableComparable {
	/**
	 * Construct a Point with the given dimensions [dim]. The coordinates should
	 * all be 0. For example: Constructing a Point(2) should create a point (x_0
	 * = 0, x_1 = 0)
	 */

	ArrayList<Float> coordinates;

	public Point() {
		this.coordinates = new ArrayList<Float>();
	}

	public Point(int dim) {
		this.coordinates = new ArrayList<Float>(dim);
	}

	/**
	 * Construct a point from a properly formatted string (i.e. line from a test
	 * file)
	 * 
	 * @param str
	 *            A string with coordinates that are space-delimited. For
	 *            example: Given the formatted string str="1 3 4 5" Produce a
	 *            Point {x_0 = 1, x_1 = 3, x_2 = 4, x_3 = 5}
	 */
	public Point(String str) {
		// System.out.println("TODO");
		if (str != null) {
			this.coordinates = new ArrayList<Float>();
			// System.exit(1);
			if (this.coordinates != null) {
				String array[] = str.split("\\s+");
				for (int i = 0; i < array.length; i++) {
					Float coord = Float.parseFloat(array[i]);
					this.coordinates.add(coord);
				}
			}
		}
	}

	/**
	 * Copy constructor
	 */
	public Point(Point other) {
		// System.out.println("TODO");
		// System.exit(1);
		this.coordinates = new ArrayList<Float>();
		if (other != null) {
			if (other.coordinates != null) {
				coordinates.addAll(other.coordinates);
			}
		}

	}

	/**
	 * @return The dimension of the point. For example, the point [x=0, y=1] has
	 *         a dimension of 2.
	 */
	public int getDimension() {
		// System.out.println("TODO");
		// System.exit(1);
		return this.coordinates.size();
	}

	/**
	 * Converts a point to a string. Note that this must be formatted EXACTLY
	 * for the autograder to be able to read your answer. Example: Given a point
	 * with coordinates {x=1, y=1, z=3} Return the string "1 1 3"
	 */
	public String toString() {
		// System.out.println("TODO");
		// System.exit(1);
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < this.coordinates.size(); i++) {
			result.append(coordinates.get(i).toString() + " ");
		}
		// Remove the last extra space
		return result.toString().trim();
	}

	/**
	 * One of the WritableComparable methods you need to implement. See the
	 * Hadoop documentation for more details. Comparing two points of different
	 * dimensions should return that they are different and anything else is
	 * undefined behavior.
	 */
	@Override
	public int compareTo(Object o) {
		// System.out.println("TODO");
		// System.exit(1);
		if (o instanceof Point) {
			Point p = (Point) o;
			if (this.getDimension() != p.getDimension()) {
				System.out.println("Cannot compare points of different dimension. Calling exit.");
				System.exit(-1);
			} else {
				for (int i = 0; i < this.coordinates.size(); i++) {
					Float diff = this.coordinates.get(i) - p.coordinates.get(i);
					if(Math.abs(diff) <= 0.000001){
						//equal
					}
					else if (this.coordinates.get(i) > p.coordinates.get(i)) {
						return 1;
					} else if ((this.coordinates.get(i) < p.coordinates.get(i))) {
						return -1;
					}
				}
				return 0;
			}

		} else {
			System.exit(-1);
		}
		return -1;

	}

	/**
	 * @return The L2 distance between two points.
	 */
	public static final float distance(Point x, Point y) {
		// What if dimensions are different?
		Float sum = (float) 0.0;
		for (int i = 0; i < x.getDimension(); i++) {
			Float diff = x.coordinates.get(i) - y.coordinates.get(i);
			sum =  (float) (sum + Math.pow(diff, 2));
		}
		return (float) Math.sqrt(sum);

	}

	/**
	 * @return A new point equal to [x]+[y]
	 */
	public static final Point addPoints(Point x, Point y) {
		if (x.coordinates.size() != y.coordinates.size()) {
			System.out.println("Error . Different dimensions");
			System.exit(1);
		} else {
			Point p = new Point(x.coordinates.size());
			for (int i = 0; i < x.coordinates.size(); i++) {
				Float sum = x.coordinates.get(i) + y.coordinates.get(i);
				p.coordinates.add(i, sum);
			}
			return p;
		}

		return null;
	}

	/**
	 * @return A new point equal to [c][x]
	 */
	public static final Point multiplyScalar(Point x, float c) {
		Point p = new Point(x.coordinates.size());
		for (int i = 0; i < x.coordinates.size(); i++) {
			Float current = x.coordinates.get(i);
			p.coordinates.add(i, current * c);

		}
		return p;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for (int i = 0; i < this.coordinates.size(); i++) {
			result = result + Float.floatToIntBits(coordinates.get(i));
		}
		return prime * result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		//this.coordinates.clear();
		int count =  in.readInt(); 

		this.coordinates = new ArrayList<Float>(count);
		for(int i = 0; i < count; i++){
			Float coord = in.readFloat();
			this.coordinates.add(coord);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.coordinates.size());  
		for (Float x: this.coordinates) {
			out.writeFloat(x);
		}
	}
}
