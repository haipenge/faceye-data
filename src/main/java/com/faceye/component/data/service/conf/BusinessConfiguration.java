package com.faceye.component.data.service.conf;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusinessConfiguration {
	private static final Logger logger = LoggerFactory.getLogger(BusinessConfiguration.class);

	private BusinessConfiguration() {

	}

	public static Configuration create() {
		Configuration conf = new Configuration();
		addBusinessConfiguration(conf);
		return conf;
	}

	private static Configuration addBusinessConfiguration(Configuration conf) {
//		conf.addResource("/tools/hadoop/hadoop-2.6.0/share/faceye/conf/business-default.xml");
		
//		conf.addResource("/tools/hadoop/hadoop-2.6.0/share/faceye/conf/business-site.xml");
//		conf.addResource("/tools/hadoop/hadoop-2.6.0/share/faceye/conf/mongo-defaults.xml");
//		conf.addResource("business-default.xml");
//		conf.addResource("business-site.xml");
//		conf.addResource("mongo-defaults.xml");
		
		conf.addResource("conf/business-default.xml");
		conf.addResource("conf/business-site.xml");
		conf.addResource("conf/mongo-defaults.xml");
		
//		RunJar run=null;
//		FileSystem fs;
//		try {
//			fs = FileSystem.getLocal(conf);
//			String newJarPath = new Path("lib").makeQualified(fs).toString();
//			String tmpjars = conf.get("tmpjars");
//			logger.debug(">>FaceYe --> tempJar is:"+tmpjars);
//			logger.debug(">>FaceYe --> new Jar path is :"+newJarPath);
//			if (tmpjars == null || tmpjars.length() == 0) {
//				conf.set("tmpjars", newJarPath);
//			} else {
//				conf.set("tmpjars", tmpjars + "," + newJarPath);
//			}
//		} catch (IOException e) {
//			logger.error(">>FaceYe throws Exception: --->"+e.toString());
//		}
		
		
		/////////////////////////////////////////////////////////////////
//		 String tableName = otherArgs[0].trim();
//         String input = otherArgs[1].trim();
//        
//         // set table columns
//         conf.set("table.cf.family", "cf_basic");
//         conf.set("table.cf.qualifier.fqdn", "domain");
//         conf.set("table.cf.qualifier.timestamp", "create_at");
//                  
//         Job job = new Job(conf, "Import into HBase table");
//         job.setJarByClass(DataImporter.class);
//         job.setMapperClass(ImportFileLinesMapper.class);
//         job.setOutputFormatClass(TableOutputFormat.class);
//        
//         job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
//         job.setOutputKeyClass(ImmutableBytesWritable.class);
//         job.setOutputValueClass(Put.class);
//        
//         job.setNumReduceTasks(0);
//        
//         FileInputFormat.addInputPath(job, new Path(input));

		return conf;
	}

}
