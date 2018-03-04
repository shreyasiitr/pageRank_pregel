import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

public class PageRank {
	private int num;
	private int[] source;
	private int[] dest;
	// Cyclic barrier for synchronizing threads with the current superstep.
	public CyclicBarrier barrier;
	public ArrayList<Integer> superStep = new ArrayList<Integer>();
	// Result map to store the final vertex values
	private ConcurrentHashMap<Integer, Double> res = new ConcurrentHashMap<Integer, Double>();
	// Map to find if a thread has halted
	private ConcurrentHashMap<Integer, Integer> halt = new ConcurrentHashMap<Integer, Integer>();
	// Map to store the messages passed between threads mapped to their vertex ID
	private ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>> incoming = new ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>>();

	public PageRank(int size, int[] fromVertices, int[] toVertices) {
		num = size;
		source = fromVertices;
		dest = toVertices;
		superStep.add(1);
		for (int i = 0; i < num; i++) {
			ArrayList<ArrayList<Double>> t = new ArrayList<ArrayList<Double>>();
			ArrayList<Double> q = new ArrayList<Double>();
			t.add(q);
			incoming.put(i, t);
		}
		barrier = new CyclicBarrier(num, new Runnable() {
			public void run() {
				superStep.set(0, superStep.get(0) + 1);
			}
		});

	}

	public void run() throws InterruptedException {
		for (int i = 0; i < num; i++) {
			new Thread(new Runnable() {
				private int tid;
				private int tnum;
				private int[] tsource;
				private int[] tdest;
				private CyclicBarrier br;
				private ArrayList<Integer> step = new ArrayList<Integer>();
				private ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>> tincoming = new ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>>();
				private ConcurrentHashMap<Integer, Double> result = new ConcurrentHashMap<Integer, Double>();

				@Override
				public void run() {
					PageRankVertex p = new PageRankVertex(tid, tnum, tincoming);
					// Run the loop till all vertices have halted
					while (halt.size() < tnum) {
						int tempStep;
						synchronized (this) {
							// Get the current superstep
							tempStep = step.get(0);
						}
						if (tempStep > 1) {
							// If a vertex has not halted, then compute the new value from messages from the
							// last superstep.
							if (p.getHalt() == 0) {
								ArrayList<Double> message = new ArrayList();
								ArrayList<ArrayList<Double>> mTemp = new ArrayList<ArrayList<Double>>();
								mTemp = tincoming.get(tid);
								try {
									for (int k = 0; k < mTemp.size(); k++) {
										if (mTemp.get(k).size() > 0) {
											// Only the messages from the last superstep have to be used for value
											// calculation
											if (mTemp.get(k).get(0) == (double) (tempStep - 1)) {
												message.add(mTemp.get(k).get(1));
											}
										}
									}
								} catch (Exception e) {

								}
								synchronized (this) {
									// Send the received messages for calculation
									p.compute(message);
									// Check if vertex is supposed to halt
									p.voteToHalt();
								}
							}
							// If vertex has halted, then add the ID to halt map
							else if (!halt.containsKey(tid)) {
								halt.put(tid, 1);
							}
						}
						int check = 0;
						int outNum = 0;
						// Check if the vertex has outgoing edges
						for (int j = 0; j < tsource.length; j++) {
							if (tsource[j] == tid) {
								outNum++;
								check = 1;
							}
						}
						// If the vertex is disconnected, then send to all vertices
						if (check == 0) {
							for (int l = 0; l < num; l++) {
								synchronized (this) {
									p.sendMessageTo(l, (double) (p.getValue() / tnum), (double) tempStep);
								}
							}
						}
						// Send messages to all outgoing edges
						else {
							for (int j = 0; j < tsource.length; j++) {
								if (tsource[j] == tid) {
									synchronized (this) {
										p.sendMessageTo(tdest[j], (double) (p.getValue() / outNum), (double) tempStep);
									}
								}
							}
						}
						try {
							// Wait for other threads to complete the superstep
							br.await();
						} catch (InterruptedException e) {

						} catch (BrokenBarrierException e) {

						}
					}
					// Store the final value in the map
					result.put(tid, p.getValue());
					if (result.size() == tnum) {
						for (int l = 0; l < tnum; l++) {
							System.out.println(result.get(l));
						}
					}
					br.reset();
					return;
				}

				public Runnable init(int pid, int pnum, int[] psource, int[] pdest,
						ConcurrentHashMap<Integer, Double> res,
						ConcurrentHashMap<Integer, ArrayList<ArrayList<Double>>> m, ArrayList<Integer> st,
						CyclicBarrier barrier) {
					tid = pid;
					tnum = pnum;
					tsource = psource;
					tdest = pdest;
					tincoming = m;
					step = st;
					br = barrier;
					result = res;
					return this;
				}
			}.init(i, num, source, dest, res, incoming, superStep, barrier)).start();
		}
	}

	public static void main(String[] args) {
		// Graph has vertices from 0 to `size-1`
		// and edges 1->0, 2->0, 3->0
		int size = 5;
		int[] from = {1,2,3 };
		int[] to = { 0, 0, 0 };

		PageRank pr = new PageRank(size, from, to);

		try {
			pr.run();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
