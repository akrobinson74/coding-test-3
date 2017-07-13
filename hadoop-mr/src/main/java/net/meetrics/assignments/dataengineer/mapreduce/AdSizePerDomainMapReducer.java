package net.meetrics.assignments.dataengineer.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class AdSizePerDomainMapReducer extends AbstractTool {
    private static final Logger LOGGER =
        Logger.getLogger(AdSizePerDomainMapReducer.class);

    public static class AdSizeMapper
        extends Mapper<Object, Text, Text, Text> {

        private static final Map<String, String> DOMAINCODE_MAP =
            new HashMap<>();

        private String adSize, domainValue;

        @Override
        public void map(
            Object key,
            Text value,
            Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            domainValue =
                DOMAINCODE_MAP.getOrDefault(fields[0], "");
            adSize = fields[11];

            context.write(
                new Text(domainValue), new Text(adSize));
        }

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException {
            super.setup(context);
            loadDomainCodeMap();
        }

        private void loadDomainCodeMap() {
            String data;
            try {
                BufferedReader bufferedReader =
                    new BufferedReader(
                        new FileReader(
                            new File("../datasrc/csv",
                                "domain_codes.csv")));

                while ((data = bufferedReader.readLine()) != null) {

                    String[] columns = data.split(",");

                    if (columns.length > 1)
                        DOMAINCODE_MAP.put(
                            columns[0].trim(), columns[1].trim());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static class JobReducer extends Reducer<Text, Text, Text, Text> {
        private String avgAdSize;

        public void reduce(
            Text key,
            Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

            int count = 0, height = 0, width = 0;
            for (Text value : values) {
                String adSize = value.toString();

                String[] dimensions =
                    adSize.equals("") ? new String[]{"0","0"} :
                        adSize.split("x");
                width += Integer.parseInt(dimensions[0]);
                height += Integer.parseInt(dimensions[1]);

                count++;
            }

            avgAdSize = String.format(
                "%dx%d", Math.round(width / count), Math.round(height / count));

            context.write(
                key,
                new Text(avgAdSize));
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(
            ToolRunner.run(
                new Configuration(),
                new AdSizePerDomainMapReducer(),
                args)
        );
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, "ThisJob");
        job.setJarByClass(AdSizePerDomainMapReducer.class);
        job.setMapperClass(AdSizeMapper.class);
        job.setCombinerClass(JobReducer.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
