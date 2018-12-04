package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class SpillableQueue<E> {

	public static void main(String[] args) throws Exception {
		SpillableQueue<Integer> q = new SpillableQueue<Integer>(5, new WrapSerializer(new IntSerializer()));
		int begin = 10;
		int count = 10;
		for (int i = begin; i < begin + count; i++) {
			System.out.println("\u001B[32m" + "Put: " + i + "\u001B[0m");
			q.put(i);
		}
		for (int i = 0; i < count / 2; i++) {
			System.out.println("\u001B[31m" + "Take: " + q.take() + "\u001B[0m");
		}
		for (int i = begin; i < begin + count; i++) {
			System.out.println("\u001B[32m" + "Put: " + i + "\u001B[0m");
			q.put(i);
		}
		for (int i = 0; i < count + count / 2; i++) {
			System.out.println("\u001B[31m" + "Take: " + q.take() + "\u001B[0m");
		}
	}

	private BackDrainableQueue<E> head;
	private BackDrainableQueue<E> tail;
	private LinkedBlockingQueue<SpilledBuffer<E>> spilled;

	private int M;

	private ItemSerializer<E> serializer;

	final ReentrantLock lock;

	SpillableQueue(Integer M, ItemSerializer<E> serializer) {
		this.M = M;
		this.serializer = serializer;
		head = new BackDrainableQueue<E>(2*M);
		tail = new BackDrainableQueue<E>(M);
		spilled = new LinkedBlockingQueue<>();
		lock = new ReentrantLock(true);
	}

	public boolean offer(E item, long timeout, TimeUnit unit) throws IOException, InterruptedException {
		put(item);
		return true;
	}

	void put(E item) throws InterruptedException, IOException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		BackDrainableQueue<E> cur;
		try {
			if (spilled.isEmpty()) {
				cur = head;
			} else {
				cur = tail;
			}
			if(cur.remainingCapacity() == 0) {
				System.out.println("Write-OUT");
				SpilledBuffer<E> buf = new SpilledBuffer<E>(M, serializer);
				cur.drainBackTo(buf.getRecords(), M);
				buf.writeOut();
				spilled.put(buf);
				System.out.println("Number of spilled buffers:" + spilled.size());
//				System.out.println("Space left:" + cur.remainingCapacity());
				cur = tail;
			}
			cur.put(item);
		} finally {
			lock.unlock();
		}
	}

	public E poll(long timeout, TimeUnit unit) throws IOException, InterruptedException {
		readIn();
		E item = head.poll(timeout, unit);
		return item;
	}

	E take() throws InterruptedException, IOException {
		readIn();
		E item = head.take();
		return item;
	}

	private void readIn() throws InterruptedException, IOException {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (head.isEmpty()) {
				if (!spilled.isEmpty()) {
					System.out.println("Read-IN");
					SpilledBuffer<E> buf = spilled.take();
					buf.readIn();
//					System.out.println("Reading from spill: " + buf.getRecords().size() + " records");
					System.out.println("Number of spilled buffers:" + spilled.size());
					head.addAll(buf.getRecords());
				}
				if (spilled.isEmpty() && !tail.isEmpty()) {
					System.out.println("Transfer");
					tail.drainTo(head);
				}
			}
		} finally {
			lock.unlock();
		}
	}
}

class BackDrainableQueue<E> extends ArrayBlockingQueue<E>{

	public BackDrainableQueue(int capacity) {
		super(capacity);
	}

	int drainBackTo(Collection<? super E> c, int maxElements) {
		Iterator<E> it = this.iterator();
		int queueSize = this.size();
		int toSkip = queueSize - maxElements;
		int removed = 0;
//		System.out.println("Queue size: " + queueSize + " cap: " + this.remainingCapacity() + " Skip size: " + toSkip + " maxElements: " + maxElements);
		for (int i = 0; i < queueSize; i++) {
			E item = it.next();
			if (i < toSkip) {
//				System.out.println("Iter: " + i + " Skipped: " + item);
				continue;
			}
			c.add(item);
			it.remove();
//			System.out.println("Iter: " + i + " Drained: " + item);
			removed++;
		}
		return removed;
	}
}

interface ItemSerializer<E> {
	public void serialize(E value, DataOutputView target) throws IOException;
	public E deserialize(DataInputView source) throws IOException;
}

class WrapSerializer implements ItemSerializer<Integer> {

	private IntSerializer ser;

	WrapSerializer(IntSerializer ser) {
		this.ser = ser;
	}

	@Override
	public void serialize(Integer value, DataOutputView target) throws IOException {
		ser.serialize(value, target);
	}

	@Override
	public Integer deserialize(DataInputView source) throws IOException {
		return ser.deserialize(source);
	}
}

class SpilledBuffer<E> {

	private int size;
	private ItemSerializer<E> serializer;
	private ArrayList<E> records;
	private File tempFile;
	DataOutputViewStreamWrapper out;
	DataInputViewStreamWrapper in;

	SpilledBuffer(int size, ItemSerializer<E> serializer) throws IOException {
		this.size = size;
		this.serializer = serializer;
		records = new ArrayList<E>(size);

		tempFile = File.createTempFile("spill-", ".tmp");
		tempFile.deleteOnExit();
		out = new DataOutputViewStreamWrapper(new BufferedOutputStream(new FileOutputStream(tempFile)));
		in = new DataInputViewStreamWrapper(new BufferedInputStream(new FileInputStream(tempFile)));
	}

	void writeOut() throws IOException {
		System.out.println("Writing to disk: " + records.size() + " records");
		for (E record: records) {
			serializer.serialize(record, out);
		}
		out.flush();
		records.clear();
	}

	void readIn() throws IOException {
		for (int i = 0; i < size; i++) {
			E record = serializer.deserialize(in);
			records.add(record);
		}
		System.out.println("Read from disk: " + records.size() + " records");
	}

	ArrayList<E> getRecords() {
		return records;
	}
}
