package Interface;

public interface Logger {

	void push(String message) throws InterruptedException;

	void poll() throws InterruptedException;

	void tokenRefill();
	
	void storeLog(String message);
}
