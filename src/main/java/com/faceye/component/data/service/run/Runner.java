package com.faceye.component.data.service.run;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.faceye.component.data.service.mapreduce.example.ExampleRunner;
import com.faceye.component.data.service.stock.mapreduce.ComputeDailyDataRunner;
import com.faceye.component.data.service.stock.mapreduce.InitStockRunner;



/**
 * 计算入口
 * @author @haipenge 
 * haipenge@gmail.com
*  Create Date:2015年2月23日
 */
public class Runner {
	private Logger logger = LoggerFactory.getLogger(Runner.class);

	public static void main(String[] args) throws Exception {
		Runner runner = new Runner();
		runner.run(args);
	}

	public Runner() {
		// Configuration conf = new Configuration();
		// conf.addResource("mongo-defaults.xml");
		// // conf.addResource("/tools/hadoop/hadoop-2.6.0/share/faceye/conf/business-default.xml");
		// conf.addResource("business-default.xml");
		// conf.addResource("business-site.xml");
		// this.setConf(conf);
	}

	public int run(String[] args) throws IOException {
		int res = 0;
		// Configuration conf = BusinessConfiguration.create();
		logger.debug(">>FaceYE --> Start Runner now.");
		// logger.debug(">>FaceYe --> is conf is null?" + (null == getConf()));
		// GenericOptionsParser optionParser = new GenericOptionsParser(getConf(), args);
		// String[] remainingArgs = optionParser.getRemainingArgs();
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
			// logger.debug(">>FaceYe -->dfs.namenode.name.dir:" + getConf().get("dfs.namenode.name.dir"));
			// logger.debug(">>FaceYe -->,business.hdfs.dir is:" + getConf().get("business.hdfs.dir"));
			String runnerClass = ExampleRunner.class.getName();
			if (StringUtils.equals(args[0], "stock")) {
				runnerClass = InitStockRunner.class.getName();
				logger.debug(">>FaceYe --Runner class is:" + runnerClass);
				res = ToolRunner.run(new InitStockRunner(), args);
			} else if (StringUtils.equals(args[0], "stock-avg")) {
				res = ToolRunner.run(new ComputeDailyDataRunner(1260L), args);
			} else if (StringUtils.equals(args[0], "stocks")) {
			} else {
				res = ToolRunner.run(new ExampleRunner(), args);
			}
		} catch (Exception e) {
			logger.error(">>FaceYe throws Exception: --->{}", e);
		}
		return res;
	}
}
