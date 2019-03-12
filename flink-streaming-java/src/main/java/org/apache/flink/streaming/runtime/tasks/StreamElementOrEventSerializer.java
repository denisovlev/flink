package org.apache.flink.streaming.runtime.tasks;

import org.apache.commons.io.IOUtils;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is able to serialize records and checkpoint barrier events
 * @param <IN> Type of the records
 */
public class StreamElementOrEventSerializer<IN> implements ItemSerializer<Either<StreamRecord<IN>, CheckpointBarrier>> {

	private static final int STREAM_ELEMENT = 0;

	private static final int STREAM_EVENT = 1;

	private static final int CHECKPOINT_TYPE_CHECKPOINT = 0;

	private static final int CHECKPOINT_TYPE_SAVEPOINT = 1;

	private StreamElementSerializer recordSerializer;

	StreamElementOrEventSerializer(StreamElementSerializer serializer) {
		this.recordSerializer = serializer;
	}


	@Override
	public void serialize(Either<StreamRecord<IN>, CheckpointBarrier> value, DataOutputViewStreamWrapper target) throws IOException {
		if (value.isLeft()) {
			serializeRecord(value.left(), target);
		} else {
			serializeEvent(value.right(), target);
		}
	}

	private void serializeEvent(CheckpointBarrier barrier, DataOutputViewStreamWrapper target) throws IOException {
		target.writeInt(STREAM_EVENT);
		ByteBuffer buf = EventSerializer.toSerializedEvent(barrier);
		byte[] bytesArr = buf.array();
		target.write(bytesArr, 0, bytesArr.length);
	}

	private void serializeRecord(StreamRecord<IN> record, DataOutputViewStreamWrapper target) throws IOException {
		target.writeInt(STREAM_ELEMENT);
		recordSerializer.serialize(record, target);
	}

	@Override
	public Either<StreamRecord<IN>, CheckpointBarrier> deserialize(DataInputViewStreamWrapper source) throws IOException {
		int type = source.readInt();
		Either<StreamRecord<IN>, CheckpointBarrier> result;
		if (type == STREAM_ELEMENT) {
			StreamRecord<IN> record = deserializeRecord(source);
			result = Either.Left(record);
		} else if (type == STREAM_EVENT) {
			CheckpointBarrier event = deserializeEvent(source);
			result = Either.Right(event);
		} else {
			throw new RuntimeException("Unsupported serialized value");
		}
		return result;
	}

	private CheckpointBarrier deserializeEvent(DataInputViewStreamWrapper source) throws IOException {
//		ByteBuffer buf = ByteBuffer.wrap(IOUtils.toByteArray(source));
//		return (CheckpointBarrier) EventSerializer.fromSerializedEvent(buf, this.getClass().getClassLoader());
		return deserializeCheckpointBarrier(source);
	}

	private StreamRecord<IN> deserializeRecord(DataInputViewStreamWrapper source) throws IOException {
		return (StreamRecord<IN>) recordSerializer.deserialize(source);
	}

	//	EventSerializer cannot work with a stream, d'oh
	// extracted part of code from EventSerializer and modified for working with streams
	private static CheckpointBarrier deserializeCheckpointBarrier(DataInputViewStreamWrapper buffer) throws IOException {
		buffer.readInt(); // read the event type == checkpoint
		final long id = buffer.readLong();
		final long timestamp = buffer.readLong();

		final int checkpointTypeCode = buffer.readInt();
		final int locationRefLen = buffer.readInt();

		final CheckpointType checkpointType;
		if (checkpointTypeCode == CHECKPOINT_TYPE_CHECKPOINT) {
			checkpointType = CheckpointType.CHECKPOINT;
		} else if (checkpointTypeCode == CHECKPOINT_TYPE_SAVEPOINT) {
			checkpointType = CheckpointType.SAVEPOINT;
		} else {
			throw new IOException("Unknown checkpoint type code: " + checkpointTypeCode);
		}

		final CheckpointStorageLocationReference locationRef;
		if (locationRefLen == -1) {
			locationRef = CheckpointStorageLocationReference.getDefault();
		} else {
			byte[] bytes = new byte[locationRefLen];
			buffer.read(bytes);
			locationRef = new CheckpointStorageLocationReference(bytes);
		}

		return new CheckpointBarrier(id, timestamp, new CheckpointOptions(checkpointType, locationRef));
	}
}
