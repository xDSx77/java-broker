package fr.epita.broke;

public class Message {

    private static Integer _nextId = 0;

    public final Integer id = _nextId++;
    public final String message;

    public Message(String message){
        this.message = message;
    }
}
