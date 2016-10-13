package de.hhu.bsinfo.dxram.script;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by nothaas on 10/13/16.
 */
public class ScriptTest {

	public void method() {
		System.out.println("ScriptTest method");
	}

	public static void main(String[] args) throws ScriptException, NoSuchMethodException {
		ScriptEngineManager man = new ScriptEngineManager();

		ScriptEngine engine = man.getEngineByName("JavaScript");

		ScriptTest test = new ScriptTest();
		engine.put("dxram", test);

		try {
			try {
				engine.eval(new FileReader("test.js"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} catch (ScriptException e) {
			e.printStackTrace();
		}

		Invocable inv = (Invocable) engine;

		inv.invokeFunction("myfunc", "11111");
	}
}
