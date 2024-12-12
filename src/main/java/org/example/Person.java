package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Person {
        private String name;
        private Integer age; // Integer to allow for null values

        private Date hireUtilDate;

        // Getters and setters
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

        public Date getHireUtilDate() {
            return hireUtilDate;
        }

        public void setHireUtilDate(Date hireUtilDate) {
            this.hireUtilDate = hireUtilDate;
        }


    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", hireUtilDate=" + hireUtilDate +
                '}';
    }

    public static MapFunction<Row, Person> mapper = row -> {
            Person person = new Person();

            String nameStr= row.getAs("name");
            if(nameStr!=null && !nameStr.isBlank()) {
                person.setName(nameStr);
            }

            String ageStr =  row.getAs("age");
            if(ageStr!=null && !ageStr.isBlank()) {
                person.setAge(Integer.valueOf(ageStr));
            }

            String dateStr = row.getAs("hire_date");
            if(dateStr!=null && !dateStr.isBlank()) {
                java.util.Date date = new SimpleDateFormat("MM-yyyy-dd").parse(dateStr);
                person.setHireUtilDate(date);;
            }
            return person;
        };

    }