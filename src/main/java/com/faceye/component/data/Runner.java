package com.faceye.component.data;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.hbase.job.runner.ExampleRunner;
import com.faceye.component.data.mysql.runner.Stock2HDFS;
import com.faceye.component.data.mysql.runner.StockRunner;
import com.faceye.component.data.spark.Bootstrap;
import com.faceye.component.data.spark.stream.CheckRecoderGenerator;
import com.faceye.component.data.spark.stream.output.StatCompanyConsumer;
import com.faceye.component.data.spark.stream.output.StatRecordConsumer;

public class Runner {
	private Logger logger = LoggerFactory.getLogger(Runner.class);

	public static void main(String[] args) throws Exception {
		Runner runner = new Runner();
		runner.run(args);
	}

	public int run(String[] args) throws IOException {
		int res = 0;
		// Configuration conf = BusinessConfiguration.create();
		logger.debug(">>FaceYE --> Start Runner now.");
		if (args != null) {
			StringBuilder sb = new StringBuilder();
			sb.append(">>FaceYe --> Runner args :");
			for (int i = 0; i < args.length; i++) {
				String arg = args[i];
				sb.append("{" + i + "=");
				sb.append(arg);
				sb.append("}  ");
			}
			// sb.append("\n>>FaceYe -->end of args print.");
			logger.debug(sb.toString());
		}
		try {
			String runnerClass = Stock2HDFS.class.getName();
			if (StringUtils.equals(args[0], "stock")) {
				runnerClass = StockRunner.class.getName();
				logger.debug(">>FaceYe --Runner class is:" + runnerClass);
				res = ToolRunner.run(new StockRunner(), args);
			} else if (StringUtils.equals(args[0], "hbase")) {
				runnerClass = ExampleRunner.class.getName();
				logger.debug(">>FaceYe --Runner class is:" + runnerClass);
				res = ToolRunner.run(new ExampleRunner(), args);
			} else if (StringUtils.equals(args[0], "spark")) {
				System.out.println(">>Start to run spark app.");
				Bootstrap bootstrap = new Bootstrap();
				bootstrap.run();
			} else if (StringUtils.equals(args[0], "consumer-stat-record")) {
				StatRecordConsumer consumer = new StatRecordConsumer();
				consumer.consumer();
			} else if (StringUtils.equals(args[0], "consumer-stat-company")) {
				StatCompanyConsumer consumer = new StatCompanyConsumer();
				consumer.consumer();
			}else if(StringUtils.equals(args[0], "stream-express-delivery-topic")){
				CheckRecoderGenerator g=new CheckRecoderGenerator();
				g.generator();
			}
		} catch (Exception e) {
			logger.error(">>FaceYe throws Exception: --->{}", e);
		}
		return res;
	}
}
