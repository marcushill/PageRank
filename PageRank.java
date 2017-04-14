import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.ClassNotFoundException;
import java.lang.InterruptedException;
import java.util.Map;
import java.util.HashMap;

public class PageRank {

    private static void writeStringToFile(String s, FSDataOutputStream out) throws IOException {
        out.writeChars(s);
        out.writeChars(" ");
    }

    private static double makeInitialInputFile(FileSystem fs, Path inputFile, Path tempDir) throws IOException {
        //read the file
        FSDataInputStream inFile = fs.open(inputFile);
        int numNodes = Integer.parseInt(inFile.readLine().trim());
        double convergence = Double.parseDouble(inFile.readLine().trim());

        boolean canRead = true;
        String line;
        String[] tokens;
        Map<String, List<String>> temp = new HashMap<>();

        while (canRead) {
            try {
                line = inFile.readLine();
                if (line == null) {
                    break;
                }
                tokens = line.split(" ");
                temp.computeIfAbsent(tokens[0], k -> new ArrayList<>(numNodes));
                temp.get(tokens[0]).add(tokens[1]);
            } catch (EOFException e) {
                canRead = false;
            }
        }

        FSDataOutputStream out = fs.create(new Path(tempDir, "input"), true);
        for (Map.Entry<String, List<String>> entry : temp.entrySet()) {
            writeStringToFile(entry.getKey(), out);
            writeStringToFile(entry.getKey(), out);
            writeStringToFile(String.valueOf(1.0 / numNodes), out);
            for (String outlink : entry.getValue()) {
                writeStringToFile(outlink, out);
            }
            out.writeChars("\n");
        }
        out.hflush();
        return convergence;
    }

    private static void prepOutput(Configuration conf, String outputFile) throws IOException {
        final Path output = new Path(outputFile);
        final Path tmpFile = new Path(output, "out/part-r-00000");
        final FileSystem fs = FileSystem.get(conf);

        FSDataInputStream inFile = fs.open(tmpFile);
        boolean canRead = true;
        String line;
        String[] tokens;
        Map<String, String> map = new HashMap<>();
        while (canRead) {
            try {
                line = inFile.readLine();
                if (line == null) {
                    break;
                }
                tokens = line.split(" ");
                map.put(tokens[0], tokens[2]);
            } catch (EOFException e) {
                canRead = false;
            }
        }
        fs.delete(output, true);
        FSDataOutputStream out = fs.create(output);
        for(Map.Entry<String, String> entry : map.entrySet()){
            out.writeChars(entry.getKey());
            out.writeChar(' ');
            out.writeChars(entry.getValue());
            out.writeChar('\n');
        }
        out.close();
    }

    public static void runPageRank(Configuration conf, Path inputFile, Path tmpDir)
            throws IOException, InterruptedException, ClassNotFoundException {
        final FileSystem fs = FileSystem.get(conf);

        //setup input/output directories
        final Path inDir = new Path(tmpDir, "in");
        final Path outDir = new Path(tmpDir, "out");
        if (fs.exists(tmpDir)) {
            throw new IOException("Tmp directory " + fs.makeQualified(tmpDir)
                    + " already exists.  Please remove it first.");
        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }

        //read the file
        double convergence = makeInitialInputFile(fs, inputFile, inDir);
        System.out.println("Finished writing input files");
        boolean needsNewRun = true;
        int currentIteration = 0;
        conf.setDouble("convergence", convergence);

        Path jobInputFile = new Path(inDir, "input");

        // //start the jobs loop
        while (needsNewRun) {
            // setup temp directories
            System.out.println("Starting Loop");
            Job job = setUpJob(conf, currentIteration, jobInputFile, outDir);
            System.out.println("Starting Job: " + currentIteration);
            job.waitForCompletion(false);
            ++currentIteration;
            needsNewRun = job.getCounters().findCounter(CounterKeys.NEEDS_RUN).getValue() > 0;
            if(needsNewRun) {
                fs.delete(jobInputFile, true);
                FileUtil.copyMerge(fs, outDir, fs, jobInputFile, true, conf, "");
//                fs.delete(outDir, true);
            } else {
                System.out.println("DONE!");
                fs.close();
            }
        }
    }


    public static Job setUpJob(Configuration conf, int iteration, Path inputFile, Path outputFile)
            throws IOException {
        Job job = Job.getInstance(conf, "pageRank: " + iteration);
        // setUpJob(job, args[0], args[1]);
        job.setJarByClass(PageRank.class);
        job.setInputFormatClass(PageRankInputFormat.class);
        //job.setInputFormatClass(KeyValueTextInputFormat.class);


        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageRankWrapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // turn off speculative execution, because DFS doesn't handle
        // multiple writers to the same file.
        job.setSpeculativeExecution(false);

        System.out.println(inputFile.toString());
        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        final long startTime = System.currentTimeMillis();
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        try {
            runPageRank(conf, new Path(args[0]), new Path(args[1]));
        } catch (RemoteException ex){
            // pass
            // I know this is a bad idea, but in this particular case everything seems to be
            // correct and Hadoop seems to want to close a file that doesn't exist anymore.
        }
        prepOutput(conf, args[1]);
        final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
        System.out.println("Here");
        System.out.println("Job Finished in " + duration + " seconds");
    }
}
