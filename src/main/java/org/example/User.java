package org.example;

import java.sql.Date;

public class User {
        private String name;
        private Integer age; // Integer to allow for null values

        private Date hireSqlDate;

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

        public Date getHireSqlDate() {
            return hireSqlDate;
        }

        public void setHireSqlDate(Date hireSqlDate) {
            this.hireSqlDate = hireSqlDate;
        }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", hireSqlDate=" + hireSqlDate +
                '}';
    }
}