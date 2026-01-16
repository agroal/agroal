// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.fakeserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Logger.getLogger;

public class MySqlFakeConnection implements Runnable {

    private static final Logger logger = getLogger( MySqlFakeConnection.class.getName() );
    private static final String SERVER_VERSION = "8.0.30-fake";
    private final Socket socket;
    private final int connId;
    private final ServerBehavior behavior;
    private volatile boolean shuttingDown;

    public MySqlFakeConnection(Socket socket, int connId, ServerBehavior behavior) {
        this.socket = socket;
        this.connId = connId;
        this.behavior = behavior;
    }

    private boolean checkStopProcessing() {
        return Thread.interrupted() || shuttingDown;
    }

    public void run() {
        try (InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {

            // STEP 1: Send Initial Handshake Packet
            byte[] handshakePacket = behavior.sendInitialHandshake(createHandshakePacket(connId));
            logPacket("[CONN-" + connId + "] SEND Initial Handshake", handshakePacket);
            out.write(handshakePacket);
            out.flush();

            if(checkStopProcessing()){
                return;
            }

            // STEP 2: Receive Handshake Response from client
            byte[] responsePacket = behavior.receivedInitialHandshakeResponse(readPacket(in));

            if(checkStopProcessing()){
                return;
            }

            if (responsePacket != null) {
                logPacket("[CONN-" + connId + "] RECV Handshake Response", responsePacket);
                parseHandshakeResponse(responsePacket, connId);

                // STEP 3: Send OK packet to complete authentication
                byte[] okPacket = behavior.sendCompleteAuthentication(createOKPacket(2)); // sequence 2
                logPacket("[CONN-" + connId + "] SEND OK Packet (Authentication Success)", okPacket);
                out.write(okPacket);
                out.flush();

                logger.info("[CONN-" + connId + "] Handshake completed and Connection established. Waiting for commands...");

                // STEP 4: Handle commands in a loop
                int commandSequence = 0;
                while (true) {
                    byte[] commandPacket = readPacket(in);
                    if(checkStopProcessing()){
                        return;
                    }
                    if (commandPacket == null) {
                        logger.info("[CONN-" + connId + "] Client disconnected");
                        return;
                    }

                    logPacket("[CONN-" + connId + "] RECV Command Packet", commandPacket);

                    // Parse command and respond appropriately
                    handleCommand(commandPacket, out, connId, commandSequence);
                    commandSequence = 0; // Reset for next command
                }
            }

        } catch (Exception e) {
            if (!(checkStopProcessing() && "Socket closed".equals(e.getMessage()))) {
                logger.log(Level.SEVERE, "[CONN-" + connId + "] Error: " + e.getClass().getSimpleName(), e);
            }
        } finally {
            this.close();
        }
    }

    /**
     * Creates MySQL Protocol::HandshakeV10 packet
     * Structure according to MySQL documentation:
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
     */
    private byte[] createHandshakePacket(int connectionId) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();

        // 1. Protocol version (1 byte) - always 10 for HandshakeV10
        payload.write(10);
        logger.fine("[GENERATE] Protocol version: 10");

        // 2. Server version string (null-terminated)
        payload.write(SERVER_VERSION.getBytes());
        payload.write(0); // null terminator
        logger.fine("[GENERATE] Server version: " + SERVER_VERSION);

        // 3. Connection ID (4 bytes, little-endian)
        payload.write(intToBytes(connectionId, 4));
        logger.fine("[GENERATE] Connection ID: " + connectionId);

        // 4. Auth plugin data part 1 (8 bytes) - scramble/salt for authentication
        byte[] authPluginData1 = "12345678".getBytes();
        payload.write(authPluginData1);
        logger.fine("[GENERATE] Auth plugin data part 1: 12345678");

        // 5. Filler (1 byte) - always 0x00
        payload.write(0);

        // 6. Capability flags lower 2 bytes (little-endian)
        // CRITICAL: Must include CLIENT_PLUGIN_AUTH (0x00080000) for modern JDBC drivers
        int capabilities = 0x00000001 |  // CLIENT_LONG_PASSWORD
                0x00000002 |  // CLIENT_FOUND_ROWS
                0x00000004 |  // CLIENT_LONG_FLAG
                0x00000008 |  // CLIENT_CONNECT_WITH_DB
                0x00000200 |  // CLIENT_PROTOCOL_41
                0x00002000 |  // CLIENT_TRANSACTIONS
                0x00008000 |  // CLIENT_SECURE_CONNECTION
                0x00080000 |  // CLIENT_PLUGIN_AUTH (REQUIRED!)
                0x00200000;   // CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA

        int capabilityLower = capabilities & 0xFFFF;
        payload.write(shortToBytes(capabilityLower));
        logger.fine("[GENERATE] Capability flags: 0x" + Integer.toHexString(capabilities));

        // 7. Character set (1 byte) - utf8mb4_general_ci = 45
        payload.write(45);
        logger.fine("[GENERATE] Character set: 45 (utf8mb4_general_ci)");

        // 8. Status flags (2 bytes, little-endian) - SERVER_STATUS_AUTOCOMMIT
        payload.write(shortToBytes(0x0002));
        logger.fine("[GENERATE] Status flags: 0x0002 (AUTOCOMMIT)");

        // 9. Capability flags upper 2 bytes (little-endian)
        int capabilityUpper = (capabilities >> 16) & 0xFFFF;
        payload.write(shortToBytes(capabilityUpper));

        // 10. Auth plugin data length (1 byte)
        // If CLIENT_PLUGIN_AUTH is set, this is the length of auth_plugin_data
        // Total will be 21 bytes (8 + 13)
        payload.write(21);
        logger.fine("[GENERATE] Auth plugin data length: 21");

        // 11. Reserved (10 bytes) - all 0x00
        payload.write(new byte[10]);

        // 12. Auth plugin data part 2 (at least 13 bytes)
        // Total auth data = 8 + 13 = 21 bytes
        byte[] authPluginData2 = "1234567890123".getBytes();
        payload.write(authPluginData2);
        logger.fine("[GENERATE] Auth plugin data part 2: 1234567890123");

        // 13. Auth plugin name (null-terminated) - REQUIRED when CLIENT_PLUGIN_AUTH is set
        String authPluginName = "mysql_native_password";
        payload.write(authPluginName.getBytes());
        payload.write(0); // null terminator
        logger.fine("[GENERATE] Auth plugin name: " + authPluginName);


        byte[] payloadBytes = payload.toByteArray();

        // Wrap payload in MySQL packet format (3 bytes length + 1 byte sequence + payload)
        return wrapInMySQLPacket(payloadBytes, 0);
    }

    /**
     * Handles MySQL commands and sends appropriate responses
     */
    private void handleCommand(byte[] packet, OutputStream out, int connId, int startSeq) throws IOException {
        if (packet.length < 5) {
            logger.warning("Invalid packet (too short)");
            return;
        }

        int commandType = packet[4] & 0xFF;
        String commandName = getCommandName(commandType);
        logger.fine("[CONN-" + connId + "] Command: 0x" + String.format("%02x", commandType) + " (" + commandName + ")");
        String commandText = "";
        if (packet.length > 5) {
            byte[] commandData = Arrays.copyOfRange(packet, 5, packet.length);
            commandText = new String(commandData).trim();
            logger.fine("[CONN-" + connId + "] Query: " + commandText);
        }

        var command = behavior.receivedCommandRequest( new ServerBehavior.Command(commandType, commandName, commandText));
        byte[] response;

        if (command.commandType() == 0x01) { // COM_QUIT
            logger.info("[CONN-" + connId + "] Client requested disconnect");
            return;
        }

        // Handle specific queries that JDBC driver sends
        if (command.commandType() == 0x03) { // COM_QUERY
            response = handleQuery(command.commandText(), out, connId, startSeq);
        }else{
            // For other commands, send OK packet
            byte[] okPacket = createOKPacket(startSeq + 1);
            logPacket("[CONN-" + connId + "] SEND OK Response", okPacket);
            response = okPacket;
        }

        response = behavior.sendCommandResponse(response);

        out.write(response);
        out.flush();
    }

    /**
     * Handles SQL queries and returns appropriate result sets or OK packets
     */
    private byte[] handleQuery(String query, OutputStream out, int connId, int startSeq) throws IOException {
        String queryUpper = query.toUpperCase().trim();

        // Handle SHOW VARIABLES queries
        if (queryUpper.startsWith("SHOW VARIABLES") || queryUpper.startsWith("SHOW SESSION VARIABLES")) {
            logger.info("[CONN-" + connId + "] SEND REPLY: Returning empty result set for SHOW VARIABLES");
            return generateEmptyResultSet(out, startSeq + 1);
        }

        // Handle SELECT @@ queries (server variables) - need to return correct values
        if (queryUpper.startsWith("SELECT @@")) {
            logger.info("[CONN-" + connId + "] SEND REPLY: Handling SELECT @@ query");
            return handleSelectVariable(query, out, connId, startSeq + 1);
        }

        // Handle SELECT DATABASE()
        if (queryUpper.contains("SELECT DATABASE()")) {
            logger.info("[CONN-" + connId + "] SEND REPLY: Returning NULL for SELECT DATABASE()");
            return generateNullResult(out, startSeq + 1);
        }

        // Handle SET commands
        if (queryUpper.startsWith("SET ")) {
            logger.info("[CONN-" + connId + "] SEND REPLY: Accepting SET command");
            byte[] okPacket = createOKPacket(startSeq + 1);
            return okPacket;
        }

        // Default: send OK packet for any other query
        logger.info("[CONN-" + connId + "] SEND REPLY: Sending OK for query: " + query);
        byte[] okPacket = createOKPacket(startSeq + 1);
        return okPacket;
    }

    /**
     * Handles SELECT @@variable queries and returns appropriate values
     */
    private byte[] handleSelectVariable(String query, OutputStream out, int connId, int startSeq) throws IOException {
        String queryLower = query.toLowerCase().trim();
        String value = "unknown";

        // Transaction isolation level
        if (queryLower.contains("@@tx_isolation") || queryLower.contains("@@transaction_isolation")) {
            value = "REPEATABLE-READ"; // MySQL default
            logger.info("[CONN-" + connId + "] Returning transaction isolation: " + value);
        }
        // Character set variables
        else if (queryLower.contains("@@character_set_client") ||
                queryLower.contains("@@character_set_connection") ||
                queryLower.contains("@@character_set_results") ||
                queryLower.contains("@@character_set_server")) {
            value = "utf8mb4";
            logger.info("[CONN-" + connId + "] Returning character set: " + value);
        }
        // Collation variables
        else if (queryLower.contains("@@collation_connection") ||
                queryLower.contains("@@collation_server")) {
            value = "utf8mb4_general_ci";
            logger.info("[CONN-" + connId + "] Returning collation: " + value);
        }
        // Auto-commit
        else if (queryLower.contains("@@autocommit")) {
            value = "1";
            logger.info("[CONN-" + connId + "] Returning autocommit: " + value);
        }
        // SQL mode
        else if (queryLower.contains("@@sql_mode") || queryLower.contains("@@session.sql_mode")) {
            value = "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";
            logger.info("[CONN-" + connId + "] Returning sql_mode: " + value);
        }
        // Max allowed packet
        else if (queryLower.contains("@@max_allowed_packet")) {
            value = "67108864";
            logger.info("[CONN-" + connId + "] Returning max_allowed_packet: " + value);
        }
        // System time zone
        else if (queryLower.contains("@@system_time_zone")) {
            value = "UTC";
            logger.info("[CONN-" + connId + "] Returning system_time_zone: " + value);
        }
        // Time zone
        else if (queryLower.contains("@@time_zone")) {
            value = "SYSTEM";
            logger.info("[CONN-" + connId + "] Returning time_zone: " + value);
        }
        // Version
        else if (queryLower.contains("@@version")) {
            value = SERVER_VERSION;
            logger.info("[CONN-" + connId + "] Returning version: " + value);
        }
        // Version comment
        else if (queryLower.contains("@@version_comment")) {
            value = "Fake MySQL Server";
            logger.info("[CONN-" + connId + "] Returning version_comment: " + value);
        }
        // Default for any other variable
        else {
            value = "1";
            logger.info("[CONN-" + connId + "] Returning default value for unknown variable: " + value);
        }

        return generateSingleValueResult(out, startSeq, value, query);
    }

    /**
     * Sends a single value result (one column, one row)
     */
    private byte[] generateSingleValueResult(OutputStream out, int startSeq, String value, String query) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // Extract column name from query (simplified)
        String columnName = "result";
        if (query.toLowerCase().contains("@@")) {
            int atPos = query.toLowerCase().indexOf("@@");
            String remaining = query.substring(atPos + 2).trim();
            int endPos = remaining.indexOf(' ');
            if (endPos == -1) endPos = remaining.indexOf(',');
            if (endPos == -1) endPos = remaining.length();
            columnName = remaining.substring(0, endPos).trim();
            if (columnName.isEmpty()) columnName = "result";
        }

        // 1. Column count = 1
        byte[] colCount = wrapInMySQLPacket(new byte[]{0x01}, startSeq++);
        out.write(colCount);

        // 2. Column definition
        buffer.reset();
        buffer.write(0x03); // catalog length
        buffer.write("def".getBytes());
        buffer.write(0x00); // schema = ""
        buffer.write(0x00); // table = ""
        buffer.write(0x00); // org_table = ""
        buffer.write((byte) columnName.length()); // name length
        buffer.write(columnName.getBytes());
        buffer.write(0x00); // org_name = ""
        buffer.write(0x0c); // length of fixed fields
        buffer.write(new byte[]{0x3f, 0x00}); // character set (binary)
        buffer.write(new byte[]{(byte)0xff, 0x00, 0x00, 0x00}); // column length
        buffer.write(0xfd); // type: VAR_STRING
        buffer.write(new byte[]{0x00, 0x00}); // flags
        buffer.write(0x00); // decimals
        buffer.write(new byte[]{0x00, 0x00}); // filler

        byte[] colDef = wrapInMySQLPacket(buffer.toByteArray(), startSeq++);
        out.write(colDef);

        // 3. EOF packet after column definitions
        byte[] eof = createEOFPacket(startSeq++);
        out.write(eof);

        // 4. Row data
        buffer.reset();
        byte[] valueBytes = value.getBytes();
        buffer.write((byte) valueBytes.length); // length-encoded string
        buffer.write(valueBytes);
        byte[] row = wrapInMySQLPacket(buffer.toByteArray(), startSeq++);
        out.write(row);

        // 5. Final EOF packet after rows
        byte[] finalEof = createEOFPacket(startSeq);
        return finalEof;
    }

    /**
     * Sends an empty result set (0 rows)
     */
    private byte[] generateEmptyResultSet(OutputStream out, int startSeq) throws IOException {
        // Column count = 0
        byte[] columnCount = wrapInMySQLPacket(new byte[]{0x00}, startSeq);
        return columnCount;
    }

    /**
     * Sends a NULL result (single column, single row with NULL value)
     */
    private byte[] generateNullResult(OutputStream out, int startSeq) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // 1. Column count = 1
        byte[] colCount = wrapInMySQLPacket(new byte[]{0x01}, startSeq++);
        out.write(colCount);

        // 2. Column definition
        buffer.reset();
        buffer.write(0x03); // catalog = "def"
        buffer.write("def".getBytes());
        buffer.write(0x00); // schema = ""
        buffer.write(0x00); // table = ""
        buffer.write(0x00); // org_table = ""
        buffer.write(0x08); // name = "DATABASE()"
        buffer.write("DATABASE".getBytes());
        buffer.write(0x00); // org_name = ""
        buffer.write(0x0c); // length of fixed fields
        buffer.write(new byte[]{0x3f, 0x00}); // character set
        buffer.write(new byte[]{0x00, 0x00, 0x00, 0x00}); // column length
        buffer.write(0xfd); // type: VAR_STRING
        buffer.write(new byte[]{0x00, 0x00}); // flags
        buffer.write(0x00); // decimals
        buffer.write(new byte[]{0x00, 0x00}); // filler

        byte[] colDef = wrapInMySQLPacket(buffer.toByteArray(), startSeq++);
        out.write(colDef);

        // 3. EOF packet (if not using CLIENT_DEPRECATE_EOF)
        byte[] eof = createEOFPacket(startSeq++);
        out.write(eof);

        // 4. Row data (NULL)
        byte[] row = wrapInMySQLPacket(new byte[]{(byte)0xfb}, startSeq++); // 0xfb = NULL
        out.write(row);

        // 5. Final EOF packet
        byte[] finalEof = createEOFPacket(startSeq);
        return finalEof;
    }

    /**
     * Creates MySQL EOF packet
     */
    private byte[] createEOFPacket(int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        buffer.write(0xfe); // EOF header
        buffer.write(shortToBytes(0x0000)); // warnings
        buffer.write(shortToBytes(0x0002)); // status flags (AUTOCOMMIT)

        return wrapInMySQLPacket(buffer.toByteArray(), sequenceId);
    }

    /**
     * Creates MySQL OK packet with sequence ID
     */
    private byte[] createOKPacket(int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        buffer.write(0x00); // OK header
        buffer.write(0x00); // affected rows
        buffer.write(0x00); // last insert id
        buffer.write(shortToBytes(0x0002)); // status flags (AUTOCOMMIT)
        buffer.write(shortToBytes(0x0000)); // warnings

        return wrapInMySQLPacket(buffer.toByteArray(), sequenceId);
    }

    /**
     * Reads a MySQL packet from input stream
     */
    private byte[] readPacket(InputStream in) throws IOException {
        byte[] header = new byte[4];
        int bytesRead = 0;

        while (bytesRead < 4) {
            if(checkStopProcessing()){
                return new byte[]{};
            }
            int read = in.read(header, bytesRead, 4 - bytesRead);
            if (read == -1) {
                return null;
            }
            bytesRead += read;
        }

        int length = (header[0] & 0xFF) |
                ((header[1] & 0xFF) << 8) |
                ((header[2] & 0xFF) << 16);
        int sequenceId = header[3] & 0xFF;

        byte[] payload = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            if(checkStopProcessing()){
                return new byte[]{};
            }
            int read = in.read(payload, totalRead, length - totalRead);
            if (read == -1) {
                return null;
            }
            totalRead += read;
        }

        byte[] fullPacket = new byte[4 + length];
        System.arraycopy(header, 0, fullPacket, 0, 4);
        System.arraycopy(payload, 0, fullPacket, 4, length);

        return fullPacket;
    }

    /**
     * Parses handshake response (simplified version)
     */
    private void parseHandshakeResponse(byte[] packet, int connId) {
        String username = "";
        if (packet.length > 36) {
            int offset = 36; // Skip to username
            StringBuilder usernameBuilder = new StringBuilder();
            while (offset < packet.length && packet[offset] != 0) {
                usernameBuilder.append((char) packet[offset]);
                offset++;
            }
            username = usernameBuilder.toString();
        }
        logger.fine("[CONN-" + connId + "] Parsing Handshake Response -> Username: '" + username + "'");
    }

    /**
     * Gets command name from command byte
     */
    private String getCommandName(int commandType) {
        switch (commandType) {
            case 0x00: return "COM_SLEEP";
            case 0x01: return "COM_QUIT";
            case 0x02: return "COM_INIT_DB";
            case 0x03: return "COM_QUERY";
            case 0x04: return "COM_FIELD_LIST";
            case 0x0e: return "COM_PING";
            case 0x16: return "COM_STMT_PREPARE";
            case 0x17: return "COM_STMT_EXECUTE";
            case 0x19: return "COM_STMT_CLOSE";
            case 0x1f: return "COM_RESET_CONNECTION";
            default: return "UNKNOWN_" + commandType;
        }
    }

    /**
     * Logs packet in hex format
     */
    /**
     * Logs packet in hexadecimal and ASCII format
     */
    private void logPacket(String label, byte[] packet) {
        logger.info(label + "( Length: " + packet.length + " bytes)");

        StringWriter sw = new StringWriter();

        for (int i = 0; i < packet.length; i += 16) {
            // Offset
            sw.append(String.format("%04x: ", i));

            // Hex bytes
            for (int j = 0; j < 16; j++) {
                if (i + j < packet.length) {
                    sw.append(String.format("%02x ", packet[i + j] & 0xFF));
                } else {
                    sw.append("   ");
                }
            }

            sw.append(" | ");

            // ASCII representation
            for (int j = 0; j < 16 && i + j < packet.length; j++) {
                byte b = packet[i + j];
                if (b >= 32 && b < 127) {
                    sw.append((char) b);
                } else {
                    sw.append(".");
                }
            }

            sw.append("\n");
        }
        logger.fine("Hex dump:\n" + sw);
    }


    /**
     * Wraps payload in MySQL packet format (length + sequence + payload)
     */
    private byte[] wrapInMySQLPacket(byte[] payload, int sequenceId) throws IOException {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();

        // Payload length (3 bytes, little-endian)
        int length = payload.length;
        packet.write(length & 0xFF);
        packet.write((length >> 8) & 0xFF);
        packet.write((length >> 16) & 0xFF);

        // Sequence ID (1 byte)
        packet.write(sequenceId);

        // Payload
        packet.write(payload);

        return packet.toByteArray();
    }
    // Helper methods for byte conversions

    private static byte[] intToBytes(int value, int numBytes) {
        byte[] bytes = new byte[numBytes];
        for (int i = 0; i < numBytes; i++) {
            bytes[i] = (byte) ((value >> (i * 8)) & 0xFF);
        }
        return bytes;
    }

    private static byte[] shortToBytes(int value) {
        return new byte[] {
                (byte) (value & 0xFF),
                (byte) ((value >> 8) & 0xFF)
        };
    }

    private static int bytesToInt(byte[] bytes, int offset, int numBytes) {
        int value = 0;
        for (int i = 0; i < numBytes && offset + i < bytes.length; i++) {
            value |= (bytes[offset + i] & 0xFF) << (i * 8);
        }
        return value;
    }
    /**
     * Converts bytes to hex string
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02x ", b & 0xFF));
        }
        return hex.toString();
    }


    public void close() {
        if(socket != null && !socket.isClosed()) {
            try {
                socket.close();
                logger.info("[CONN-" + connId + "] Connection closed");
            } catch (IOException e) {
                logger.log(Level.SEVERE, "[CONN-" + connId + "] Error during connection close: " + e.getClass().getSimpleName(), e);
            }
        }
    }

    public void initShutdown() {
        logger.info("[CONN-" + connId + "] Init shutdown");
        shuttingDown = true;
    }
}
