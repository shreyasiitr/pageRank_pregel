import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class PageRankVertex implements Vertex<Double, Double> {
    private static final Double damping = 0.85; // the damping ratio, as in the PageRank paper
    private static final Double tolerance = 1e-4; // the tolerance for converge checking
    public int total;
    //Current value of vertex
    public Double val;
    //Previous value of vertex
    private Double prev;
    //Vertex ID
    public int id;
    //Variable to determine if vertex has halted computation
    private int halt = 0;
    ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>> verticesQueue = new ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>>();
    public PageRankVertex(int a, int b, ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>>e) {
    		val = (double)(1/(double)b);
    		prev = (double)-1;
    		total = b;
    		id = a;
    		verticesQueue = e;
    }
    @Override
    public int getVertexID() {
        return id;
    }
    @Override
    public Double getValue() {
        return val;
    }

    @Override
    public void compute(Collection<Double> messages) {
    		Double sum = 0.0;
    		for (Double elem : messages) {
            sum += elem;
        }
    		//New value calculation
    		prev = val;
    		val = 0.15/(double)total + 0.85*sum;
    }

    @Override
    public void sendMessageTo(int vertexID, Double message, Double step) {
    		//Message queue element contains both the superstep and the value in the form of an array list
    		ArrayList<Double> a = new ArrayList();
    		a.add(step);
    		a.add(message);
    		ArrayList<ArrayList<Double>> temp = new ArrayList<ArrayList<Double>>();
    		temp = verticesQueue.get(vertexID);
    		temp.add(a);
    		//Adding the new value in the queue of the desired vertex
    		verticesQueue.put(vertexID, temp);
    }

    @Override
    public void voteToHalt() {
    		//Check to see if the value has converged
    		if(prev!=(double)(-1)) {
    			if( Math.abs(val-prev)<tolerance) {
    				halt = 1;
    			}
    		}
    }
    
    public int getHalt() {
    		return halt;
    }
    
    @Override
    public void run() {

    }
}
