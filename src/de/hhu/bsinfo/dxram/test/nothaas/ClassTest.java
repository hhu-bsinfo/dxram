package de.hhu.bsinfo.dxram.test.nothaas;

public class ClassTest 
{
	public static class A 
	{
		public A()
		{
			System.out.println(this.getClass().getName());
			System.out.println(this.getClass().getSimpleName());
			System.out.println(this.getClass().getSuperclass().getName());
		}
	}
	
	public static class B extends A
	{
		public B()
		{
			System.out.println(this.getClass().getName());
			System.out.println(this.getClass().getSimpleName());
		}
	}
	
	public static void main(String[] args)
	{
		//A a = new A();
		A a2 = new B();
	}
	
	public void blub()
	{
		System.out.println(this.getClass().getName());
		System.out.println(this.getClass().getSimpleName());
	}
}
