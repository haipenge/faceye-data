package com.faceye.component.data.service.stock.mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;

public class ComputeDailyDataReducer extends Reducer<DailyDataKey, BSONWritable, NullWritable, MongoUpdateWritable> {
	private Log logger = LogFactory.getLog(getClass());
	private Integer[] DAYS = new Integer[] { 5, 10, 20, 30, 60, 120, 250 };
	DecimalFormat    df   = new DecimalFormat("######0.00");  
	@Override
	protected void reduce(DailyDataKey key, Iterable<BSONWritable> values, Context context) throws IOException, InterruptedException {
		logger.debug(">>FaceYe run reduce of key:" + key);
		List<BSONWritable> items = new ArrayList<BSONWritable>();
		if (values != null) {
			Iterator<BSONWritable> ir = values.iterator();
			while (ir.hasNext()) {
				BSONWritable bsonWritable = new BSONWritable();
				bsonWritable.setDoc(ir.next().getDoc());
				 logger.info(">>FaceYe --> stock id:"+bsonWritable.getDoc().get("stockId")+",dailyData id :"+bsonWritable.getDoc().get("_id")+bsonWritable.getDoc().get("date"));
				items.add(bsonWritable);
			}
		}
		Iterator<BSONWritable> it = items.iterator();
		int index = -1;
		// for(int i=0;i<items.size();i++){
		// BasicBSONObject modifiers = new BasicBSONObject();
		// BSONWritable bsonWritable=items.get(i);
		// BSONObject bson=bsonWritable.getDoc();
		// Long id=new Long(bson.get("_id").toString());
		// logger.info(">>FaceYe --> Bson _id of dailya data is:"+id);
		// BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
		// builder.add("avg5", 25D);
		// modifiers.put("$set", BasicDBObjectBuilder.start().add("avg5", 33).add("avg10", i).add("avg20",
		// Constants.TEST_REDUCT_COUNT).add("avg30",items.size()).get());
		// logger.info(">>FaceYe -->,i is:"+i+","+ "id is:"+id+",modifiers is:" + modifiers.toString());
		// BasicBSONObject query = new BasicBSONObject("_id", bson.get("_id"));
		// // query.append("stockId", stockId);
		// MongoUpdateWritable update=new MongoUpdateWritable(query, modifiers, true, false);
		// logger.debug(">>FaceYe -->Mongo update:"+update.getQuery().toString()+","+update.getModifiers().toString());
		// context.write(NullWritable.get(),update);
		// // continue;
		// }

		while (it.hasNext()) {
			index++;
			BSONWritable bsonWritable = it.next();
			BSONObject bson = bsonWritable.getDoc();
			Long id = Long.parseLong(bson.get("_id").toString());
			Double avg5 = null;
			Double avg10 = null;
			Double avg20 = null;
			Double avg30 = null;
			Double avg60 = null;
			Double avg120 = null;
			Double avg250 = null;
			for (int j = 0; j < DAYS.length; j++) {
				if (bson.containsField("avg" + j) && null != bson.get("avg" + j)) {
					Double avg = new Double(bson.get("avg" + j).toString());
					if (DAYS[j].intValue() == 5) {
						avg5 = avg;
					} else if (DAYS[j].intValue() == 10) {
						avg10 = avg;
					} else if (DAYS[j].intValue() == 20) {
						avg20 = avg;
					} else if (DAYS[j].intValue() == 30) {
						avg30 = avg;
					} else if (DAYS[j].intValue() == 60) {
						avg60 = avg;
					} else if (DAYS[j].intValue() == 120) {
						avg120 = avg;
					} else if (DAYS[j].intValue() == 250) {
						avg250 = avg;
					}
				}
			}
			// if(avg5!=null && avg10!=null && avg30!=null && avg60!=null && avg120!=null && avg250!=null){
			// continue;
			// }
			for (int j = 0; j < DAYS.length; j++) {
				int day = DAYS[j].intValue();
				int toIndex = 0;
				Double avg = 0D;
				if (day == 5) {
					if (avg5 == null) {
						toIndex = index + 5;
					}
				} else if (day == 10) {
					if (avg10 == null) {
						toIndex = index + 10;
					}
				} else if (day == 20) {
					if (avg20 == null) {
						toIndex = index + 20;
					}
				} else if (day == 30) {
					if (avg30 == null) {
						toIndex = index + 30;
					}
				} else if (day == 60) {
					if (avg60 == null) {
						toIndex = index + 60;
					}
				} else if (day == 120) {
					if (avg120 == null) {
						toIndex = index + 120;
					}
				} else if (day == 250) {
					if (avg250 == null) {
						toIndex = index + 250;
					}
				}
				if (toIndex != 0) {
					Double total = 0D;
					if (items.size() >= toIndex) {
						List<BSONWritable> sub = items.subList(index, toIndex);
						for (BSONWritable bw : sub) {
							BSONObject b = bw.getDoc();
							Double shoupanjia = null;
							if (b.containsField("shoupanjia")) {
								if (StringUtils.isNotEmpty(b.get("shoupanjia").toString())) {
									shoupanjia = Double.parseDouble(b.get("shoupanjia").toString());
								}
							}
							if (shoupanjia == null) {
								if (null != b.get("dangqianjiage") && StringUtils.isNotEmpty(b.get("dangqianjiage").toString())) {
									shoupanjia = Double.parseDouble(b.get("dangqianjiage").toString());
								}
							}
							if (null != shoupanjia) {
								total += shoupanjia;
							} else {
								total = 0D;
								// break;
							}
						}
					}
					if (total.compareTo(0D) > 0) {
						avg = total / day;
					}
					if (avg.compareTo(0D) > 0) {
						avg=Double.parseDouble(df.format(avg));
						if (day == 5) {
							avg5 = avg;
						}
						if (day == 10) {
							avg10 = avg;
						}
						if (day == 20) {
							avg20 = avg;
						}
						if (day == 30) {
							avg30 = avg;
						}
						if (day == 60) {
							avg60 = avg;
						}
						if (day == 120) {
							avg120 = avg;
						}
						if (day == 250) {
							avg250 = avg;
						}
					}
				}
			}
			// 开始更新DOC
			BasicBSONObject modifiers = new BasicBSONObject();
			BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
			int count = 0;
			if (avg5 != null) {
				builder.add("avg5", avg5);
				count++;
			}
			if (avg10 != null) {
				builder.add("avg10", avg10);
				count++;
			}
			if (avg20 != null) {
				builder.add("avg20", avg20);
				count++;
			}
			if (avg30 != null) {
				builder.add("avg30", avg30);
				count++;
			}
			if (avg60 != null) {
				builder.add("avg60", avg60);
				count++;
			}
			if (avg120 != null) {
				builder.add("avg120", avg120);
				count++;
			}
			if (avg250 != null) {
				builder.add("avg250", avg250);
				count++;
			}
			if (builder.get() != null && StringUtils.isNotEmpty(builder.get().toString()) && count != 0) {
				modifiers.put("$set", builder.get());
				logger.debug(">>FaceYe --> modifiers is:" + modifiers.toString());
				BasicBSONObject query = new BasicBSONObject("_id", new Long(id));
				context.write(NullWritable.get(), new MongoUpdateWritable(query, modifiers, true, false));
			}
		}
	}

}
