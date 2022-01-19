package com.baiyan;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

/**
 * @author baiyan
 * @date 2022/01/19
 */
@SpringBootApplication
@EnableRetry
public class KafkaSortConsumeApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSortConsumeApplication.class, args);
    }

}
