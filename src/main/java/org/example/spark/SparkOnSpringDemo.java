package org.example.spark;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching

public class SparkOnSpringDemo {

    public static void main(String[] args) {
        SpringApplication.run(SparkOnSpringDemo.class, args);
    }

}
