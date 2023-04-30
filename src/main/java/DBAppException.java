
public class DBAppException extends Exception{
    public DBAppException(){
        super();
    }
    public DBAppException(String errorMessage){
        super(errorMessage);
    }
}
