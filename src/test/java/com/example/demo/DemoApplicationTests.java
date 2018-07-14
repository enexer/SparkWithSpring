package com.example.demo;

import com.example.demo.configuration.PropertiesUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.reflect.Field;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DemoApplicationTests {

	@Before
	public void before() throws NoSuchFieldException, IllegalAccessException {
		PropertiesUtils.delimiter="asdasd";
	}
	@Test
	public void contextLoads() {
	}

}
