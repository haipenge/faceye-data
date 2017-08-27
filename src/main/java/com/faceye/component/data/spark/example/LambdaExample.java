package com.faceye.component.data.spark.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class LambdaExample {

	public void map() {
		List<Person> persons = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			Person per = new Person();
			per.setName("name" + i);
			persons.add(per);
		}
		// 类型推断
		Stream<String> res = persons.stream().map(p -> {
			return p.getName() + " - from map";
		});
		res.forEach(s -> System.out.println(s));
	}

	public static void main(String args[]) {
		LambdaExample exam = new LambdaExample();
		exam.map();
	}

	class Person {
		private String name = "";

		private Integer age = 15;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Integer getAge() {
			return age;
		}

		public void setAge(Integer age) {
			this.age = age;
		}

	}
}
