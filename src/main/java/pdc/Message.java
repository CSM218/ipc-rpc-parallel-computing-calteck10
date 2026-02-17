package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

/**
 * TLV message framing for the CSM218 protocol.
 * Header: 1 byte TYPE + 4 bytes LENGTH (big endian payload size).
 * Body:   Binary payload that includes basic metadata and opaque data bytes.
 */
public class Message {
    public static final String MAGIC = "CSM218";
    public static final int CURRENT_VERSION = 1;

    public static final byte TYPE_REGISTER = 1;
    public static final byte TYPE_TASK = 2;
    public static final byte TYPE_RESULT = 3;
    public static final byte TYPE_HEARTBEAT = 4;

    public String magic = MAGIC;
    public int version = CURRENT_VERSION;
    public String messageType;
    public String studentId = System.getenv().getOrDefault("STUDENT_ID", "student");
    public long timestamp = System.currentTimeMillis();
    public byte[] payload = new byte[0];

    /**
     * Numeric type used on the wire. Defaults to 0 when unspecified.
     */
    public byte typeCode = 0;

    /**
     * Converts this Message into a TLV-framed byte array.
     * Uses DataOutputStream to guarantee big-endian ordering.
     */
    public byte[] pack() {
        try {
            byte resolvedType = resolveTypeCode();

            ByteArrayOutputStream payloadBuffer = new ByteArrayOutputStream();
            DataOutputStream payloadOut = new DataOutputStream(payloadBuffer);

            payloadOut.writeUTF(magic);
            payloadOut.writeInt(version);
            payloadOut.writeUTF(messageType == null ? "" : messageType);
            payloadOut.writeUTF(studentId == null ? "" : studentId);
            payloadOut.writeLong(timestamp);
            payloadOut.writeInt(payload == null ? 0 : payload.length);
            if (payload != null && payload.length > 0) {
                payloadOut.write(payload);
            }

            byte[] body = payloadBuffer.toByteArray();

            ByteArrayOutputStream frameBuffer = new ByteArrayOutputStream(1 + 4 + body.length);
            DataOutputStream frameOut = new DataOutputStream(frameBuffer);
            frameOut.writeByte(resolvedType);
            frameOut.writeInt(body.length);
            frameOut.write(body);

            return frameBuffer.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reads and reconstructs a Message from a TLV-framed input stream.
     * DataInputStream.readFully handles TCP fragmentation by blocking until the
     * complete header and payload are received.
     */
    public static Message unpack(InputStream input) throws IOException {
        DataInputStream in = new DataInputStream(input);

        byte type = in.readByte();
        int length = in.readInt();
        if (length < 0) {
            throw new IOException("Negative payload length");
        }

        byte[] body = new byte[length];
        in.readFully(body);

        Message msg = new Message();
        msg.typeCode = type;

        try (DataInputStream bodyIn = new DataInputStream(new ByteArrayInputStream(body))) {
            msg.magic = bodyIn.readUTF();
            msg.version = bodyIn.readInt();
            msg.messageType = bodyIn.readUTF();
            msg.studentId = bodyIn.readUTF();
            msg.timestamp = bodyIn.readLong();

            int dataLength = bodyIn.readInt();
            if (dataLength < 0) {
                throw new IOException("Negative data length");
            }
            msg.payload = new byte[dataLength];
            if (dataLength > 0) {
                bodyIn.readFully(msg.payload);
            }
        }

        if (!MAGIC.equals(msg.magic)) {
            throw new IOException("Invalid magic value: " + msg.magic);
        }

        return msg;
    }

    /**
     * Convenience overload for byte arrays (e.g., when using buffered reads).
     */
    public static Message unpack(byte[] data) {
        try {
            return unpack(new ByteArrayInputStream(data));
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to unpack message", e);
        }
    }

    /**
     * Maps a textual messageType to the wire type code when possible.
     */
    private byte resolveTypeCode() {
        if (typeCode != 0) {
            return typeCode;
        }
        if (messageType == null) {
            return 0;
        }

        String normalized = messageType.toUpperCase(Locale.US);
        switch (normalized) {
            case "REGISTER":
                return TYPE_REGISTER;
            case "TASK":
                return TYPE_TASK;
            case "RESULT":
                return TYPE_RESULT;
            case "HEARTBEAT":
                return TYPE_HEARTBEAT;
            default:
                return 0;
        }
    }
}
