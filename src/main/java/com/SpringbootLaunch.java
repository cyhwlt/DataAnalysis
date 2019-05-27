package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class SpringbootLaunch {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(SpringbootLaunch.class, args);
	}

}