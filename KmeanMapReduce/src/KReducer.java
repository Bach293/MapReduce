import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<LongWritable, PointWritable, Text, Text> {

    private final List<PointWritable> pointsInCluster = new ArrayList<>();

    public void reduce(LongWritable centroidId, Iterable<PointWritable> partialSums, Context context)
            throws IOException, InterruptedException {

        pointsInCluster.clear();
        PointWritable ptFinalSum = PointWritable.copy(partialSums.iterator().next());
        pointsInCluster.add(PointWritable.copy(ptFinalSum));  // Thêm điểm đầu tiên
        int totalPoints = ptFinalSum.getNumPoints();

        while (partialSums.iterator().hasNext()) {
            PointWritable nextPoint = partialSums.iterator().next();
            ptFinalSum.sum(nextPoint);  // Cộng các điểm
            pointsInCluster.add(PointWritable.copy(nextPoint));  // Thêm các điểm còn lại
            totalPoints += nextPoint.getNumPoints();
        }

        ptFinalSum.calcAverage();

        // Ghi centroid và danh sách các điểm
        context.write(new Text("Centroid[" + centroidId.toString() + "]"), new Text(ptFinalSum.toString()));
        context.write(new Text("Cluster Size: "), new Text(String.valueOf(totalPoints)));
        for (PointWritable point : pointsInCluster) {
            context.write(new Text("Point: "), new Text(point.toString()));
        }
    }
}
