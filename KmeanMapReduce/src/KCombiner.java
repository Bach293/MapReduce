import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

    public void reduce(LongWritable centroidId, Iterable<PointWritable> points, Context context)
            throws IOException, InterruptedException {

        PointWritable ptSum = PointWritable.copy(points.iterator().next());  // Sao chép điểm đầu tiên.
        
        while (points.iterator().hasNext()) {
            ptSum.sum(points.iterator().next());  // Cộng dồn các điểm dữ liệu lại.
        }

        context.write(centroidId, ptSum);  // Ghi kết quả tổng hợp vào kết quả Combiner.
    }
}

/*
 * reduce(): Kết hợp các điểm dữ liệu từ Mapper cho từng cụm (ID của tâm cụm là khóa). 
 * Tổng hợp các điểm thành một tổng tạm thời.
 * Mục đích: Combiner giảm lượng dữ liệu truyền từ Mapper tới Reducer bằng 
 * cách tổng hợp các điểm dữ liệu tại Mapper.
 */
