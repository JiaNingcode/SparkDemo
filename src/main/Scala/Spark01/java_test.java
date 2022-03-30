package Spark01;

public class java_test {
    public static void main(String[] args) {

        System.out.println("http");
        int a= 10;
    }
}

class HungryPerson{
    //构造私有防止外界new
    private HungryPerson(){}

    private static HungryPerson hungryPerson;

    static {
        hungryPerson=new HungryPerson();
    }
    public static HungryPerson getHungryPerson(){
        return hungryPerson;
    }
}



class LazyPerson{
    private LazyPerson(){}

    private static LazyPerson lazyPerson;

    public static LazyPerson getInstance(){
        if (lazyPerson == null){
            lazyPerson =new LazyPerson();
        }
        return lazyPerson;
    }


}
