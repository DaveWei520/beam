package willem.weiyu.bigData.beam.wordCount;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;

import java.io.File;

public class WordCountDemo {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from("E:"+ File.separator+"test.txt"))
                .apply(Distinct.create())//创建一个处理String类型的PTransform：Distinct
        .apply(TextIO.write().to("E:"+ File.separator+"result.txt"));//输出结果
        pipeline.run().waitUntilFinish();
    }
}
