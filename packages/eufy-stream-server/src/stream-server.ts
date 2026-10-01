/**
 * Simple TCP Stream Server - Raw H.264 streaming server
 *
 * This is a simplified version of the legacy eufy-stream-server that focuses
 * exclusively on streaming raw H.264 video data over TCP connections.
 * All audio processing, MP4 fragmentation, and complex error recovery
 * have been removed for simplicity.
 */

import * as net from "node:net";
import * as fs from "node:fs";
import { EventEmitter } from "node:events";
import { Duplex } from "node:stream";
import { spawn, ChildProcessWithoutNullStreams } from "node:child_process";
import JMuxer from "jmuxer";
import { Logger, ILogObj } from "tslog";
import { ConnectionManager } from "./connection-manager";
import { H264Parser } from "./h264-parser";
import { ServerStats, StreamData } from "./types";
import {
  EufyWebSocketClient,
  DEVICE_EVENTS,
  VideoMetadata,
  AudioMetadata,
} from "@caplaz/eufy-security-client";

/**
 * Configuration options for the TCP stream server
 */
export interface StreamServerOptions {
  /** Server port number (default: 8080) */
  port?: number;
  /** Server host address (default: '0.0.0.0') */
  host?: string;
  /** Maximum number of concurrent connections (default: 10) */
  maxConnections?: number;
  /**
   * @deprecated No longer used - debug level is controlled by the logger instance.
   * If you provide a logger, it controls its own debug level.
   * If no logger is provided, the internal logger defaults to info level.
   */
  debug?: boolean;
  /** Optional external logger instance compatible with tslog Logger<ILogObj> (if not provided, uses internal tslog Logger) */
  logger?: Logger<ILogObj>;
  /** WebSocket client for receiving video data events (required for Eufy cameras) */
  wsClient: EufyWebSocketClient;
  /** Device serial number to filter events (required for Eufy cameras) */
  serialNumber: string;
}

/**
 * Simple TCP streaming server for raw H.264 video data
 *
 * This server accepts TCP connections and streams raw H.264 video data
 * to all connected clients. It provides basic connection management,
 * NAL unit parsing, and key frame detection.
 *
 * @example
 * ```typescript
 * const server = new StreamServer({
 *   port: 8080,
 *   debug: true,
 *   wsClient: eufyWebSocketClient,
 *   serialNumber: 'device123'
 * });
 *
 * server.start().then(() => {
 *   console.log('Server started and listening for video data');
 * });
 * ```
 */
export class StreamServer extends EventEmitter {
  private logger: Logger<ILogObj>;
  private options: Required<Omit<StreamServerOptions, "logger">> & {
    logger?: Logger<ILogObj>;
  };
  private server?: net.Server;
  private muxedServer?: net.Server;
  /**
   * Map of muxed-client socket → its dedicated output pipeline. Cameras
   * that deliver H.264 use an in-process JMuxer (fast, no subprocess).
   * Cameras that deliver H.265 (e.g. the SoloCam S340) go through a real
   * ffmpeg transcode pipeline instead, since JMuxer only understands
   * H.264 NAL structure and silently produces no output for H.265 input
   * (confirmed against JMuxer's own documented input format).
   */
  private muxerStreams = new Map<
    net.Socket,
    | { kind: "jmuxer"; muxer: JMuxer; duplex: Duplex }
    | {
        kind: "transcode";
        proc: ChildProcessWithoutNullStreams;
        videoPipe: NodeJS.WritableStream;
        audioPipe: NodeJS.WritableStream;
      }
  >();
  private connectionManager: ConnectionManager;
  private h264Parser: H264Parser;
  private isActive = false;
  private startTime?: Date;
  private eventRemover?: () => boolean;
  private audioEventRemover?: () => boolean;

  // Stream state management
  private livestreamIntendedState = false;
  private livestreamActualState = false;
  private startStopTimeout?: ReturnType<typeof setTimeout>;

  // Video metadata from first frame
  private videoMetadata: VideoMetadata | null = null;
  private metadataReceived = false;

  // Audio metadata from first audio frame
  private audioMetadata: AudioMetadata | null = null;

  // Client activity monitoring for battery optimization
  private lastClientActivity = 0;
  private activityCheckInterval?: ReturnType<typeof setInterval>;
  private readonly ACTIVITY_TIMEOUT = 30000; // 30 seconds of no activity

  // Statistics
  private stats = {
    framesProcessed: 0,
    bytesTransferred: 0,
    lastFrameTime: null as Date | null,
  };

  // Snapshot capture state
  private snapshotResolvers: Array<{
    resolve: (buffer: Buffer) => void;
    reject: (error: Error) => void;
    timestamp: number;
  }> = [];

  // Parameter-set cache for new client initialization.
  // H.264 uses SPS (type 7) + PPS (type 8).
  // H.265 uses VPS (type 32) + SPS (type 33) + PPS (type 34).
  private cachedSPS: Buffer | null = null;
  private cachedPPS: Buffer | null = null;
  private cachedVPS: Buffer | null = null; // H.265 Video Parameter Set

  // See IStreamServer.setNextTranscodeOptions - per-request hints for
  // the next H.265 transcode connection (bitrate/resolution/fps).
  private nextTranscodeOptions: {
    bitrate?: number;
    width?: number;
    height?: number;
    fps?: number;
    profile?: string;
  } | null = null;

  constructor(options: StreamServerOptions) {
    super();

    this.options = {
      port: options.port ?? 8080,
      host: options.host ?? "0.0.0.0",
      maxConnections: options.maxConnections ?? 10,
      debug: options.debug ?? false,
      logger: options.logger,
      wsClient: options.wsClient,
      serialNumber: options.serialNumber,
    };

    // Use external logger if provided, otherwise create internal tslog Logger
    // Note: When external logger is provided, it controls its own debug level
    this.logger =
      options.logger ??
      new Logger({
        name: "StreamServer",
        minLevel: 3, // info level - external loggers control their own debug level
      });

    this.connectionManager = new ConnectionManager(this.logger);
    this.h264Parser = new H264Parser(this.logger);

    this.setupEventHandlers();
    this.setupWebSocketListener();
  }

  /**
   * Setup event handlers for connection manager
   */
  private setupEventHandlers(): void {
    this.connectionManager.on(
      "clientConnected",
      async (connectionId, connectionInfo) => {
        this.logger.info(
          `Client connected: ${connectionId} from ${connectionInfo.remoteAddress}:${connectionInfo.remotePort}`,
        );
        this.emit("clientConnected", connectionId, connectionInfo);

        // Send cached SPS/PPS headers immediately so FFmpeg can parse the stream
        this.sendCachedHeaders(connectionId);

        // Start livestream if this is the first consumer overall
        // (TCP clients + muxer clients combined). The helper internally
        // checks `livestreamIntendedState` so re-entering on every connect
        // is a no-op once the stream is up — equivalent to the old
        // `previousCount === 0` guard but muxer-aware.
        await this.updateLivestreamStateForMuxerClients();
      },
    );

    this.connectionManager.on("clientDisconnected", async (connectionId) => {
      this.logger.info(`Client disconnected: ${connectionId}`);
      this.emit("clientDisconnected", connectionId);

      // Stop livestream only if no consumers remain.
      await this.updateLivestreamStateForMuxerClients();
    });
  }

  /**
   * Send cached SPS/PPS headers to a specific client
   * This ensures new clients can immediately decode the stream
   * @param connectionId - The connection ID to send headers to
   */
  private sendCachedHeaders(connectionId: string): void {
    const hasHeaders = this.cachedVPS || this.cachedSPS || this.cachedPPS;
    if (!hasHeaders) {
      this.logger.debug(
        `No cached parameter-set headers available for client ${connectionId}`,
      );
      return;
    }

    // H.265: send VPS → SPS → PPS (order matters for decoder initialisation)
    if (this.cachedVPS) {
      this.logger.debug(
        `Sending cached VPS header (${this.cachedVPS.length} bytes) to ${connectionId}`,
      );
      this.connectionManager.sendToClient(connectionId, this.cachedVPS);
    }

    if (this.cachedSPS) {
      this.logger.debug(
        `Sending cached SPS header (${this.cachedSPS.length} bytes) to ${connectionId}`,
      );
      this.connectionManager.sendToClient(connectionId, this.cachedSPS);
    }

    if (this.cachedPPS) {
      this.logger.debug(
        `Sending cached PPS header (${this.cachedPPS.length} bytes) to ${connectionId}`,
      );
      this.connectionManager.sendToClient(connectionId, this.cachedPPS);
    }
  }

  /**
   * Start monitoring client activity to detect idle connections
   */
  private startActivityMonitoring(): void {
    this.stopActivityMonitoring(); // Clear any existing interval

    this.activityCheckInterval = setInterval(() => {
      const now = Date.now();
      const timeSinceActivity = now - this.lastClientActivity;

      // Clean up any stale connections first
      this.cleanupStaleConnections();

      // Total consumer count = TCP video clients (snapshot, raw video) +
      // in-process muxer clients (fMP4 over the muxed port). Without
      // counting the muxers here the activity timer was killing the
      // livestream whenever the muxer was the only consumer, which broke
      // long-lived downstream rebroadcast sessions.
      const totalConsumers =
        this.connectionManager.getActiveConnectionCount() +
        this.muxerStreams.size;

      if (timeSinceActivity > this.ACTIVITY_TIMEOUT && totalConsumers === 0) {
        this.logger.info(
          `🕒 No client activity for ${Math.round(timeSinceActivity / 1000)}s and no active clients, stopping camera stream`,
        );
        this.livestreamIntendedState = false;
        this.stopActivityMonitoring();
        this.ensureLivestreamState();
      } else if (totalConsumers === 0 && this.livestreamIntendedState) {
        // Brought over from main: useful diagnostic when the stream is
        // intended to be running but everyone has temporarily detached
        // (e.g. between Rebroadcast cycles). `totalConsumers` replaces
        // the old `activeClients` so muxer clients count.
        this.logger.debug(
          `No active clients but stream is intended to run - waiting for connections`,
        );
      }
    }, 5000); // Check every 5 seconds

    this.logger.debug("Started client activity monitoring");
  }

  /**
   * Stop monitoring client activity
   */
  private stopActivityMonitoring(): void {
    if (this.activityCheckInterval) {
      clearInterval(this.activityCheckInterval);
      this.activityCheckInterval = undefined;
      this.logger.debug("Stopped client activity monitoring");
    }
  }

  /**
   * Destroy TCP connections older than 5 minutes. Previously this just
   * logged; it now actually force-disconnects the socket. Necessary because
   * when a peer ffmpeg gets SIGKILL-ed the OS sometimes never surfaces a
   * `close` event on our side, leaving a zombie connection that would keep
   * the activity-monitor interval alive forever (and spam the logs every
   * 5 seconds with "Cleaning up stale connection").
   */
  private cleanupStaleConnections(): void {
    const connectionStats = this.connectionManager.getConnectionStats();
    const now = Date.now();
    let cleanedCount = 0;

    for (const [connectionId, info] of Object.entries(connectionStats)) {
      const connectionAge = now - info.connectedAt.getTime();
      if (connectionAge > 5 * 60 * 1000) {
        this.logger.info(
          `Cleaning up stale connection: ${connectionId} (age: ${Math.round(connectionAge / 1000)}s)`,
        );
        this.connectionManager.disconnectClient(connectionId);
        cleanedCount++;
      }
    }

    if (cleanedCount > 0) {
      this.logger.debug(
        `Identified ${cleanedCount} stale connections for cleanup`,
      );
    }
  }

  /**
   * Setup WebSocket event listener for video data
   */
  private setupWebSocketListener(): void {
    this.logger.info(
      `Setting up WebSocket listener for device: ${this.options.serialNumber}`,
    );

    // Listen for livestream video data events
    this.eventRemover = this.options.wsClient.addEventListener(
      DEVICE_EVENTS.LIVESTREAM_VIDEO_DATA,
      (event) => {
        // Filter events by device serial number
        if (event.serialNumber !== this.options.serialNumber) {
          return;
        }

        // Log that we received a video data event (first few only to avoid spam)
        if (this.stats.framesProcessed < 3) {
          this.logger.debug(
            `Received video data event for ${event.serialNumber}: ${event.buffer.data.length} bytes, metadata present: ${!!event.metadata}`,
          );
          if (event.metadata) {
            this.logger.debug(
              `Video metadata: codec=${event.metadata.videoCodec}, ${event.metadata.videoWidth}x${event.metadata.videoHeight} @ ${event.metadata.videoFPS}fps`,
            );
          }
        }

        // Capture video metadata from first frame
        if (!this.metadataReceived && event.metadata) {
          this.videoMetadata = {
            videoCodec: event.metadata.videoCodec,
            videoFPS: event.metadata.videoFPS,
            videoWidth: event.metadata.videoWidth,
            videoHeight: event.metadata.videoHeight,
          };
          this.metadataReceived = true;
          this.logger.info(
            `📐 Captured video metadata: ${this.videoMetadata.videoWidth}x${this.videoMetadata.videoHeight} @ ${this.videoMetadata.videoFPS}fps, codec: ${this.videoMetadata.videoCodec}`,
          );
          this.emit("metadataReceived", this.videoMetadata);
        }

        // Mark livestream as actually running when we receive data
        if (!this.livestreamActualState) {
          this.livestreamActualState = true;
          this.logger.info(
            "📹 Livestream confirmed active - receiving video data",
          );
        }

        // Log video data events based on client activity
        const activeClients = this.connectionManager.getActiveConnectionCount();
        if (activeClients > 0) {
          this.logger.debug(
            `Received video data event for ${event.serialNumber}: ${event.buffer.data.length} bytes (${activeClients} active clients)`,
          );
        } else {
          // Log less frequently when no clients - only every 10th frame
          if (this.stats.framesProcessed % 10 === 0) {
            this.logger.debug(
              `Received video data event for ${event.serialNumber}: ${event.buffer.data.length} bytes (no active clients, frame ${this.stats.framesProcessed})`,
            );
          }
        }

        // Convert JSONBuffer to Buffer if needed
        const videoBuffer = Buffer.isBuffer(event.buffer.data)
          ? event.buffer.data
          : Buffer.from(event.buffer.data);

        // Fan-out to muxer clients (fMP4 via in-process JMuxer). Update
        // the activity clock so the inactivity timer doesn't kill the
        // livestream while muxer clients are actively consuming it.
        if (this.muxerStreams.size > 0) {
          this.lastClientActivity = Date.now();
          for (const entry of this.muxerStreams.values()) {
            try {
              if (entry.kind === "jmuxer") {
                entry.muxer.feed({ video: videoBuffer });
              } else {
                entry.videoPipe.write(videoBuffer);
              }
            } catch (e) {
              this.logger.warn(`Muxer video feed error: ${e}`);
            }
          }
        }

        // Stream the video data to raw TCP clients (snapshot service,
        // direct-video stream consumers).
        this.streamVideo(videoBuffer, Date.now(), undefined);
      },
      {
        source: "device",
        serialNumber: this.options.serialNumber,
      },
    );

    this.logger.info(
      `WebSocket listener setup complete for device: ${this.options.serialNumber}`,
    );

    // Listen for livestream audio data events
    let audioFrameCount = 0;
    this.audioEventRemover = this.options.wsClient.addEventListener(
      DEVICE_EVENTS.LIVESTREAM_AUDIO_DATA,
      (event) => {
        if (event.serialNumber !== this.options.serialNumber) {
          return;
        }

        if (!this.audioMetadata && event.metadata) {
          this.audioMetadata = event.metadata;
          this.logger.info(
            `Captured audio metadata: codec=${event.metadata.audioCodec}`,
          );
        }

        const audioBuffer = Buffer.isBuffer(event.buffer.data)
          ? event.buffer.data
          : Buffer.from(event.buffer.data);

        if (audioFrameCount < 3) {
          const hex = audioBuffer
            .subarray(0, Math.min(16, audioBuffer.length))
            .toString("hex");
          this.logger.debug(
            `Audio frame #${audioFrameCount}: ${audioBuffer.length} bytes, first bytes: ${hex}`,
          );
          audioFrameCount++;
        }

        if (this.muxerStreams.size === 0) {
          return;
        }

        // Eufy delivers AAC pre-wrapped in ADTS — JMuxer consumes ADTS
        // directly. Anything else (e.g. AudioSpecificConfig, which is the
        // 2-byte codec config packet that arrives ahead of the first frame)
        // is dropped because synthesizing an ADTS header without knowing
        // the actual sample rate/channel count would produce a stream the
        // decoder would misinterpret.
        if (!this.isAdtsFrame(audioBuffer)) {
          return;
        }

        for (const entry of this.muxerStreams.values()) {
          try {
            if (entry.kind === "jmuxer") {
              entry.muxer.feed({ audio: audioBuffer });
            } else {
              entry.audioPipe.write(audioBuffer);
            }
          } catch (e) {
            this.logger.warn(`Muxer audio feed error: ${e}`);
          }
        }
      },
      {
        source: "device",
        serialNumber: this.options.serialNumber,
      },
    );
  }

  /**
   * ADTS sync word check. Bytes 0..1 must be 0xFFFx (12-bit sync).
   */
  private isAdtsFrame(data: Buffer): boolean {
    return data.length >= 7 && data[0] === 0xff && (data[1] & 0xf0) === 0xf0;
  }

  /**
   * Ensure the livestream is in the correct state with retry logic
   */
  private async ensureLivestreamState(): Promise<void> {
    // Clear any existing timeout
    if (this.startStopTimeout) {
      clearTimeout(this.startStopTimeout);
      this.startStopTimeout = undefined;
    }

    const maxRetries = 3;
    const retryDelay = 5000; // 5 seconds

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // First, check the actual livestream status from the device
        let actualStreamingStatus = false;
        try {
          const statusResponse = await this.options.wsClient.commands
            .device(this.options.serialNumber)
            .isLivestreaming();
          actualStreamingStatus = statusResponse.livestreaming;
          this.logger.debug(
            `Current device livestream status: ${actualStreamingStatus}`,
          );
        } catch (error: any) {
          this.logger.warn(
            "Failed to check livestream status, continuing with command:",
            error.message || error,
          );
        }

        if (this.livestreamIntendedState && !actualStreamingStatus) {
          // Need to start livestream
          this.logger.info(
            `🎥 Starting livestream (attempt ${attempt}/${maxRetries})`,
          );
          await this.options.wsClient.commands
            .device(this.options.serialNumber)
            .startLivestream();
          this.logger.info("✅ Livestream start command sent successfully");

          // Set timeout to check if it actually started
          this.startStopTimeout = setTimeout(() => {
            if (this.livestreamIntendedState && !this.livestreamActualState) {
              this.logger.warn(
                "⚠️ Livestream start timeout - no video data received, will retry",
              );
              this.ensureLivestreamState();
            }
          }, 30000); // 30 seconds to receive first video data
        } else if (this.livestreamIntendedState && actualStreamingStatus) {
          // Stream is already running and we want it running - all good
          this.logger.debug("Livestream already running as desired");
        } else if (!this.livestreamIntendedState && actualStreamingStatus) {
          // Need to stop livestream
          this.logger.info(
            `🛑 Stopping livestream (attempt ${attempt}/${maxRetries})`,
          );
          await this.options.wsClient.commands
            .device(this.options.serialNumber)
            .stopLivestream();
          this.logger.info("✅ Livestream stop command sent successfully");
          this.livestreamActualState = false;
        } else {
          // Stream is not running and we don't want it running - all good
          this.logger.debug("Livestream already stopped as desired");
        }

        // Success - break out of retry loop
        break;
      } catch (error: any) {
        this.logger.warn(
          `❌ Livestream command failed (attempt ${attempt}/${maxRetries}):`,
          error.message || error,
        );

        if (attempt === maxRetries) {
          this.logger.error(
            `❌ Failed to set livestream state after ${maxRetries} attempts`,
          );
          this.emit("streamError", error);
        } else {
          // Wait before retrying
          this.logger.info(`⏳ Retrying in ${retryDelay / 1000} seconds...`);
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        }
      }
    }
  }

  /**
   * Start the TCP server
   */
  async start(): Promise<void> {
    if (this.isActive) {
      throw new Error("Server is already running");
    }

    await new Promise<void>((resolve, reject) => {
      this.server = net.createServer();

      this.server.on("connection", (socket) => {
        this.connectionManager.handleConnection(socket);
      });

      this.server.on("error", (error) => {
        this.logger.error("Server error:", error);
        this.emit("error", error);
        reject(error);
      });

      this.server.listen(this.options.port, this.options.host, () => {
        this.isActive = true;
        this.startTime = new Date();
        this.logger.info(
          `🚀 Stream server started on ${this.options.host}:${this.options.port}`,
        );
        this.emit("started");
        resolve();
      });
    });

    // Start muxed server — each client connection gets its own in-process
    // JMuxer that produces fragmented MP4 directly from the camera's raw
    // H.264 + ADTS AAC frames. No ffmpeg subprocess, no audio re-encoding.
    await new Promise<void>((resolve, reject) => {
      this.muxedServer = net.createServer((socket) => {
        this.handleMuxedClient(socket);
      });

      let started = false;
      this.muxedServer.on("error", (error) => {
        if (!started) {
          reject(error);
        } else {
          this.logger.error(`Muxed server runtime error: ${error}`);
          this.emit("error", error);
        }
      });

      this.muxedServer.listen(0, "127.0.0.1", () => {
        started = true;
        const address = this.muxedServer!.address();
        const port =
          address && typeof address === "object" ? address.port : "?";
        this.logger.info(`Muxed (fMP4) server started on port ${port}`);
        resolve();
      });
    });
  }

  /**
   * Handle a new connection to the muxed TCP server. Each client gets its
   * own in-process JMuxer instance that consumes raw H.264 NAL units and
   * ADTS AAC frames directly from the WebSocket events (no TCP detour,
   * no ffmpeg subprocess) and emits fragmented MP4 on the socket. This is
   * meaningfully faster than the previous ffmpeg-subprocess approach and
   * matches what the Eufy cameras actually deliver byte-for-byte.
   */
  private handleMuxedClient(socket: net.Socket): void {
    // Detect actual camera codec. Falls back to H265 because the only
    // camera this fork has been verified against (SoloCam S340) is
    // always H265, and metadata may not have arrived yet on a cold
    // start. If you use this against a confirmed H.264 camera, change
    // this fallback back to "H264".
    const eufyCodec = this.videoMetadata?.videoCodec ?? "H265";
    const isH265 = eufyCodec.toUpperCase().includes("265");

    if (isH265) {
      this.attachTranscodeClient(socket);
    } else {
      // `|| 15`, not `?? 15`: this camera has been confirmed (via direct
      // metadata dump) to report videoFPS as a literal 0, not
      // null/undefined - `??` doesn't fall back on 0, so it would
      // silently pass fps: 0 to JMuxer otherwise.
      const videoFps = this.videoMetadata?.videoFPS || 15;
      this.attachJMuxerClient(socket, videoFps);
    }
  }

  /**
   * H.264 path (unchanged): in-process JMuxer, no subprocess, matches
   * the Eufy stream byte-for-byte.
   */
  private attachJMuxerClient(socket: net.Socket, videoFps: number): void {
    // Always declare both tracks. The muxed client connects BEFORE the
    // first audio frame arrives from Eufy, so `audioMetadata` is null at
    // this point on a cold start; if we picked mode based on it we'd lock
    // in video-only and silently drop every audio frame thereafter.
    // JMuxer's `both` mode correctly holds audio until the video track is
    // ready, then emits both tracks into the fMP4 moov.
    const mode = "both";

    const muxer = new JMuxer({
      mode,
      fps: videoFps,
      flushingTime: 0,
      clearBuffer: false,
      debug: false,
    });

    const duplex: Duplex = muxer.createStream();
    let firstChunkLogged = false;
    duplex.on("data", (chunk: Buffer) => {
      if (!firstChunkLogged) {
        this.logger.info(
          `JMuxer emitting fMP4 (first chunk: ${chunk.length} bytes, mode=${mode}, fps=${videoFps})`,
        );
        firstChunkLogged = true;
      }
      if (!socket.destroyed) socket.write(chunk);
    });
    duplex.on("error", (err) => {
      this.logger.warn(`JMuxer duplex error: ${err.message}`);
    });

    this.muxerStreams.set(socket, { kind: "jmuxer", muxer, duplex });
    this.logger.info(
      `Muxed client attached via JMuxer (total active muxers: ${this.muxerStreams.size})`,
    );

    // This is the first consumer of the stream — bring up the livestream
    // if the stream server's TCP video clients haven't already started it.
    this.updateLivestreamStateForMuxerClients();

    const cleanup = () => {
      if (!this.muxerStreams.has(socket)) return;
      this.muxerStreams.delete(socket);
      try {
        muxer.destroy();
      } catch (e) {
        this.logger.warn(`JMuxer destroy threw during cleanup: ${e}`);
      }
      this.logger.info(
        `Muxed client detached (total active muxers: ${this.muxerStreams.size})`,
      );
      this.updateLivestreamStateForMuxerClients();
    };

    socket.on("close", cleanup);
    socket.on("error", cleanup);
  }

  /**
   * H.265 path: JMuxer can't mux HEVC (confirmed against its own docs
   * and issue tracker - it only documents H.264 input). Instead, spawn
   * a real ffmpeg process that decodes the raw H.265 Annex-B stream and
   * re-encodes it to H.264, muxing video (fd 3) and audio (fd 4) into a
   * fragmented MP4 written to stdout.
   *
   * Encoder is chosen at runtime based on what hardware is actually
   * present (see hasV4l2m2mEncoder below), so the same code works both
   * on a Raspberry Pi (real bcm2835-codec hardware H.264 encoder,
   * confirmed via /dev/video11 + "Using device /dev/video11, driver
   * 'bcm2835-codec'" in ffmpeg's own stderr) and on hosts with no
   * hardware encode path at all (e.g. a VM with a VMware SVGA II
   * virtual GPU - confirmed no libva driver, no /dev/nvidia*), which
   * fall back to software libx264. Software encode was measured at
   * ~215% CPU on a Pi 4 for a single 720p stream (confirmed via `ps
   * aux` during testing) - unsustainable there, hence not just always
   * defaulting to libx264 once hardware is available.
   */
  private attachTranscodeClient(socket: net.Socket): void {
    // Consume once - per-request hints (bitrate/resolution/fps/profile)
    // set by the caller (stream-service.ts) right before this
    // connection was expected. Without this, every connection got the
    // exact same fixed encode regardless of what was actually
    // negotiated for that specific session - confirmed via a real
    // HomeKit plugin log to be the actual root cause of "spins, then no
    // reply from camera": HAP's handleStreamRequest negotiated MAIN
    // profile / 1280x720 / 30fps / max_bit_rate 299 kbps for that
    // session, but the fixed encoder was unconditionally sending
    // Baseline / 1920x1080 / ~15fps / ~1000kbps - over 3x the bitrate
    // budget alone. Since Scrypted's homekit plugin does `-vcodec copy`
    // with zero re-encoding to match what it negotiated, whatever this
    // pipeline actually produces IS what the client receives.
    const requested = this.nextTranscodeOptions;
    this.nextTranscodeOptions = null;

    // `frag_keyframe` fragments the output MP4 only at keyframes, so the
    // downstream MP4 demuxer can't hand off any frame until a whole
    // fragment (= one full GOP) has arrived - it then delivers that
    // entire GOP as one burst, producing a stutter-then-catch-up cycle
    // whose PERIOD equals the GOP duration (confirmed: GOP=250 produced
    // ~17s stutter, GOP=15 @ ~15fps real-time capture produced ~1s
    // stutter - shrinking the GOP only shrunk the period, it didn't fix
    // the bursting). `frag_every_frame` decouples fragmentation from
    // keyframes entirely, flushing a fragment per frame, so delivery is
    // continuous regardless of GOP size - which then only needs to be
    // sized for compression efficiency, not smoothness.
    //
    // `|| 15`, not `?? 15`: this camera has been confirmed (via direct
    // metadata dump) to report videoFPS as a literal 0, not
    // null/undefined - `??` doesn't fall back on 0, which is exactly
    // why this previously collapsed to a GOP of 1 (all-intra encoding)
    // before this was fixed.
    const videoFps = this.videoMetadata?.videoFPS || 15;
    const gopSize = Math.max(30, Math.round(videoFps * 2));

    // /dev/video11 is the Broadcom bcm2835-codec hardware H.264 encoder
    // node on a Raspberry Pi (confirmed present on the Pi, confirmed
    // absent - no /dev/video* at all - on the VM). Presence is a
    // reasonable proxy for "real hardware encode is available here";
    // absence means fall back to software.
    const hasV4l2m2mEncoder = fs.existsSync("/dev/video11");

    // The camera's native resolution is 2880x1616 (confirmed via captured
    // metadata). libx264 handles that fine, but the Pi 4's hardware H.264
    // encoder does not - confirmed directly via ffmpeg stderr: it accepts
    // the device and format negotiation, then fails at
    // "VIDIOC_STREAMON failed on output context" once real frames arrive,
    // exiting with no output. The bcm2835-codec hardware encoder block's
    // supported resolution tops out well below the camera's native size
    // (its HEVC decoder is far more capable than its H.264 encoder), so
    // scale down before handing frames to it - to whatever was actually
    // requested (e.g. HomeKit's clientWidth for the viewing device),
    // clamped to 1920 as a safe ceiling for this hardware regardless of
    // what's requested.
    const hwTargetWidth = Math.min(requested?.width || 1920, 1920);
    const videoFilter = hasV4l2m2mEncoder
      ? `scale=${hwTargetWidth}:-2,format=yuv420p`
      : "format=yuv420p";
    // Default (used when no per-request bitrate hint is available - e.g.
    // Rebroadcast's own persistent prebuffer connection, which doesn't
    // go through the per-HomeKit-session hint path) raised from the
    // original 1000000: confirmed via direct measurement that the
    // camera's real native capture rate is ~14.6fps (matches our 15fps
    // target, not a throttling issue), so perceived low quality/motion
    // blur at full 1920x1080 was more likely just 1Mbps being too low
    // for that resolution. 2000000 matches Rebroadcast's own documented
    // recommendation (1920x1080, 2000Kb/s, Variable Bit Rate) - plain
    // `-b:v` with no forced min/maxrate already behaves as VBR, not CBR.
    const bitrate =
      requested?.bitrate && requested.bitrate > 0
        ? Math.round(requested.bitrate)
        : 2000000;
    // h264_v4l2m2m emits SPS/PPS in-band in the bitstream (confirmed:
    // the first NAL in the actual encoded sample data is a real SPS,
    // NAL type 7) but doesn't populate ffmpeg's out-of-band extradata
    // from it, so the muxer writes an empty `avcC` config box (confirmed
    // via hex dump: an 8-byte avcC box, i.e. zero payload). MP4/avc1
    // decoders read avcC to initialize, not in-band NALs, so the
    // browser had nothing to decode with - hence a black image despite
    // data genuinely flowing.
    //
    // `-bsf:v extract_extradata` alone did NOT fix this (confirmed:
    // avcC still empty after adding it) - with `empty_moov`, the muxer
    // writes its header essentially immediately, before any packet (and
    // therefore before a post-hoc bitstream filter) has run, so
    // extraction happens too late regardless. `-flags +global_header`
    // instead changes the ENCODER's own behavior, telling it to
    // populate extradata proactively at initialization rather than
    // relying on in-band parameter sets - this is the standard fix for
    // this exact class of streaming-muxer/hardware-encoder timing
    // mismatch. Not needed for libx264, which sets extradata correctly
    // on its own regardless.
    // Profile matters beyond just device compatibility: Scrypted's
    // HomeKit plugin does `-vcodec copy` unconditionally for live view
    // (confirmed by reading its source, plugins/homekit/src/types/
    // camera/camera-streaming-ffmpeg.ts) - it does not re-encode to
    // match whatever H.264 profile the Apple client actually negotiated
    // for that session, it just forwards whatever bytes the source
    // produces. Use whatever profile was actually requested (now
    // threaded through via setNextTranscodeOptions); Baseline remains
    // the fallback default when nothing was specified, since it's the
    // safest, most universally-supported choice.
    //
    // h264_v4l2m2m needs the numeric profile value (66/77/100 =
    // FF_PROFILE_H264_BASELINE/MAIN/HIGH), not the string form -
    // confirmed via direct test: the string form fails with "Undefined
    // constant or missing '(' in 'baseline'" for this specific hardware
    // encoder wrapper (unlike libx264, which accepts strings directly).
    const requestedProfile = (requested?.profile || "baseline").toLowerCase();
    const hwProfileNumeric =
      requestedProfile === "high"
        ? "100"
        : requestedProfile === "main"
          ? "77"
          : "66";
    const swProfileString =
      requestedProfile === "high"
        ? "high"
        : requestedProfile === "main"
          ? "main"
          : "baseline";

    const encoderArgs = hasV4l2m2mEncoder
      ? [
          "-c:v",
          "h264_v4l2m2m",
          "-profile:v",
          hwProfileNumeric,
          "-g",
          String(gopSize),
          "-b:v",
          String(bitrate),
          "-flags",
          "+global_header",
        ]
      : [
          "-c:v",
          "libx264",
          "-profile:v",
          swProfileString,
          "-preset",
          "veryfast",
          "-tune",
          "zerolatency",
          "-g",
          String(gopSize),
          "-keyint_min",
          String(gopSize),
          "-sc_threshold",
          "0",
          "-b:v",
          String(bitrate),
        ];
    // `frag_every_frame` assumes one complete encoded access unit is
    // available the instant a raw frame is submitted - true for
    // synchronous software libx264 (confirmed working cleanly on the
    // VM), but not for h264_v4l2m2m: V4L2 mem2mem hardware encoders
    // buffer/reorder frames asynchronously internally, so forcing a
    // fragment flush per input frame can flush before the hardware has
    // actually finished that frame - confirmed via ffprobe against the
    // hardware-encoded output: "missing picture in access unit with
    // size 50" / "No start code is found" (corrupt, undersized AUs),
    // and a black image in the browser despite the process not
    // crashing. Time-based fragmentation doesn't require that lockstep
    // assumption - the muxer just flushes whatever complete frames have
    // become available within the window.
    // `delay_moov`: defers writing the initial moov until the first
    // fragment is actually cut, instead of immediately at muxer open -
    // needed because h264_v4l2m2m's extradata isn't known until the
    // hardware has returned an actual encoded frame (a real round trip
    // through the V4L2 device), which happens after muxer open. Without
    // this, `-flags +global_header` and `-bsf:v extract_extradata` both
    // measurably failed to fix the empty avcC box (confirmed via hex
    // dump both times) because the header was already written before
    // extradata existed, regardless of what told the encoder/bitstream
    // to produce it.
    // `frag_keyframe` added on top of `-frag_duration` for the hardware
    // path: without it, the very first fragment can get time-cut before
    // a complete frame has round-tripped through the async hardware
    // encoder, producing one corrupted, undersized access unit right at
    // the start of every fresh connection (confirmed via ffprobe: "missing
    // picture in access unit with size 50" / "no frame!" on cold start,
    // consistently reproducible, not a one-off probe artifact). Browsers/
    // WebRTC tolerate that (skip ahead to the next keyframe), but
    // HomeKit's stricter live pipeline does not - this is what was
    // producing "spins, then no reply from camera" in the Home app.
    // `frag_keyframe` guarantees any real keyframe starts a fresh
    // fragment boundary, so the first fragment is always a complete,
    // valid frame; `-frag_duration` still handles periodic cuts between
    // keyframes for continuous delivery during the ~2s GOP.
    //
    // The ~3.5s cold-start latency this was briefly suspected of causing
    // was actually unrelated (confirmed: removing it did not reduce the
    // latency) - the real cause was ffmpeg's HEVC demuxer waiting for
    // fresh parameter sets, now fixed above by priming videoPipe with
    // cached VPS/SPS/PPS immediately on connect.
    const movflags = hasV4l2m2mEncoder
      ? "empty_moov+delay_moov+frag_keyframe+default_base_moof"
      : "frag_every_frame+empty_moov+default_base_moof";
    const extraMuxArgs = hasV4l2m2mEncoder ? ["-frag_duration", "200000"] : [];

    this.logger.info(
      `H.265 transcode using ${hasV4l2m2mEncoder ? "hardware (h264_v4l2m2m)" : "software (libx264)"} H.264 encoder`,
    );

    // The async hardware encoder pipeline occasionally (probabilistically,
    // not deterministically) produces a corrupted/undersized first access
    // unit on a fresh connection - confirmed both via our own ffprobe
    // testing ("missing picture in access unit with size 50") AND, more
    // importantly, via a REAL HomeKit plugin log showing the exact same
    // warning ("missing picture in access unit with size 47") from
    // Scrypted's own `-vcodec copy` ffmpeg reading our muxed output,
    // immediately followed by that process going completely silent
    // (zero further reads/writes) until HomeKit's ~30s timeout killed
    // it. `frag_keyframe` reduces how often this happens but evidently
    // doesn't eliminate it. Browsers/WebRTC tolerate one bad frame
    // (skip to the next keyframe) but HomeKit's decoder does not, and
    // repeatedly hit this across multiple real attempts.
    //
    // Mitigation: buffer output from a fresh transcode process instead
    // of forwarding it immediately. If the corruption signature shows
    // up in stderr within the buffering window, kill that attempt and
    // transparently respawn a fresh encoder for the SAME client socket
    // (the client never sees the failed attempt) - up to a few retries
    // before giving up and forwarding whatever we have as a last resort.
    const MAX_TRANSCODE_ATTEMPTS = 3;
    const CORRUPTION_DETECTION_WINDOW_MS = 1000;
    const CORRUPTION_PATTERN =
      /missing picture in access unit|No start code is found/i;

    const spawnAttempt = (attempt: number) => {
      const spawnTime = Date.now();
      const proc = spawn(
        "ffmpeg",
        [
          "-hide_banner",
          "-loglevel",
          "info",
          // Raw HEVC piped in has zero container-level timing (no
          // DTS/PTS), so ffmpeg's demuxer has to observe several frames
          // spread over real wall-clock time to even estimate a
          // framerate before it'll commit to declaring "Input #0" -
          // confirmed via a timestamped stderr timeline that this
          // probing step alone consumed ~3.4-3.6s of the ~4-7s cold-start
          // latency, mostly independent of data availability (fixing a
          // separate missing-cached-headers theory made no measurable
          // difference). Telling ffmpeg the framerate explicitly and
          // capping analyzeduration/probesize skips that guessing entirely.
          "-analyzeduration",
          "0",
          "-probesize",
          "32",
          "-framerate",
          String(videoFps),
          "-f",
          "hevc",
          "-i",
          "pipe:3",
          // Per-input options in ffmpeg only apply to the input they
          // precede - the video-side analyzeduration/probesize/framerate
          // above did NOT carry over to this second input (confirmed:
          // "Input #0, hevc" dropped to 283ms, but "Input #1, aac" still
          // took until +4293ms with no override here). Same treatment,
          // same reasoning, applied to the audio input.
          "-analyzeduration",
          "0",
          "-probesize",
          "32",
          "-f",
          "aac",
          "-i",
          "pipe:4",
          "-vf",
          videoFilter,
          ...encoderArgs,
          // Forces a strictly regular output cadence regardless of any
          // jitter in when the async hardware encoder actually delivers
          // frames. Without this, our own muxer logged "Non-monotonic
          // DTS in output stream 0:0" - a real, confirmed timestamp
          // irregularity in our own output, always present, separate
          // from the transient corrupted-first-access-unit issue.
          // Scrypted's HomeKit "Transcode video" debug mode (full
          // decode+re-encode) incidentally regenerates clean timestamps
          // and DOES work despite lower quality - strong evidence this
          // irregularity, not codec/profile/bitrate correctness (all
          // independently confirmed already), is what HomeKit's
          // stricter `-vcodec copy` RTP path can't tolerate.
          "-r",
          String(videoFps),
          "-fps_mode",
          "cfr",
          "-c:a",
          "copy",
          "-bsf:a",
          "aac_adtstoasc",
          "-f",
          "mp4",
          "-movflags",
          movflags,
          ...extraMuxArgs,
          "pipe:1",
        ],
        { stdio: ["ignore", "pipe", "pipe", "pipe", "pipe"] },
      ) as ChildProcessWithoutNullStreams;

      // fd 3/4 are opened by Node as duplex sockets, so once cleanup()
      // SIGKILLs ffmpeg they can emit EPIPE/ECONNRESET. Without a listener
      // that 'error' is unhandled and takes down the whole process.
      proc.stdio.forEach((stream, fd) => {
        (stream as NodeJS.EventEmitter | null)?.on("error", (err: Error) => {
          this.logger.debug(
            `Transcode ffmpeg fd ${fd} error (ignored): ${err.message}`,
          );
        });
      });

      const rawVideoPipe = proc.stdio[3] as NodeJS.WritableStream;
      const audioPipe = proc.stdio[4] as NodeJS.WritableStream;

      let firstVideoWriteAt: number | null = null;
      const videoPipe = {
        write: (chunk: any) => {
          if (firstVideoWriteAt === null) {
            firstVideoWriteAt = Date.now();
          }
          return rawVideoPipe.write(chunk);
        },
      } as NodeJS.WritableStream;

      // NOTE: previously primed the pipe here by writing cached
      // VPS/SPS/PPS directly, to address cold-start latency. That
      // turned out not to be the real latency fix (the analyzeduration/
      // probesize/framerate flags above were) - and, worse, likely
      // introduced a separate bug: feeding raw parameter-set NAL units
      // into the pipe as if they were frame data appears to get muxed
      // as a spurious, tiny standalone "access unit" (NAL headers, no
      // actual picture) - a very plausible explanation for "missing
      // picture in access unit" warnings seen downstream (confirmed via
      // a real HomeKit plugin log showing this exact warning from
      // Scrypted's own ffmpeg reading our muxed output). Removed.

      let corruptionDetected = false;
      let flushed = false;
      let bufferedChunks: Buffer[] = [];
      const flush = () => {
        if (flushed) return;
        flushed = true;
        clearTimeout(detectionTimer);
        for (const c of bufferedChunks) {
          if (!socket.destroyed) socket.write(c);
        }
        bufferedChunks = [];
      };
      const detectionTimer = setTimeout(flush, CORRUPTION_DETECTION_WINDOW_MS);

      let firstChunkLogged = false;
      proc.stdout.on("data", (chunk: Buffer) => {
        if (!firstChunkLogged) {
          const latencyMs = Date.now() - spawnTime;
          this.logger.info(
            `H.265 transcode pipeline emitting fMP4 (attempt ${attempt}, first chunk: ${chunk.length} bytes, ${latencyMs}ms after spawn)`,
          );
          firstChunkLogged = true;
        }
        if (flushed) {
          if (!socket.destroyed) socket.write(chunk);
        } else if (!corruptionDetected) {
          bufferedChunks.push(chunk);
        }
      });

      let stderrTail = "";
      proc.stderr.on("data", (chunk: Buffer) => {
        const text = chunk.toString();
        stderrTail = (stderrTail + text).slice(-4000);
        this.logger.warn(`Transcode ffmpeg stderr: ${text.trim()}`);

        if (!flushed && !corruptionDetected && CORRUPTION_PATTERN.test(text)) {
          corruptionDetected = true;
          clearTimeout(detectionTimer);
          try {
            proc.kill("SIGKILL");
          } catch (e) {
            this.logger.warn(
              `Transcode process kill threw during corruption retry: ${e}`,
            );
          }
          if (attempt < MAX_TRANSCODE_ATTEMPTS) {
            this.logger.warn(
              `Detected corrupt cold-start access unit (attempt ${attempt}/${MAX_TRANSCODE_ATTEMPTS}) - respawning transparently for the same client`,
            );
            spawnAttempt(attempt + 1);
          } else {
            this.logger.warn(
              `Corrupt cold-start access unit persisted after ${MAX_TRANSCODE_ATTEMPTS} attempts - forwarding anyway as a last resort`,
            );
            flush();
          }
        }
      });

      proc.on("error", (err) => {
        this.logger.warn(`Transcode ffmpeg process error: ${err.message}`);
      });
      proc.on("exit", (code, signal) => {
        if (code !== 0 && code !== null && !corruptionDetected) {
          this.logger.warn(
            `Transcode ffmpeg exited with code ${code} (signal ${signal}). Last stderr: ${stderrTail}`,
          );
        }
      });

      // Always (re)point the map entry at whichever process is
      // currently live, so cleanup() below always kills the right one
      // regardless of how many retries have happened - including the
      // very first attempt, where this is the initial registration.
      this.muxerStreams.set(socket, {
        kind: "transcode",
        proc,
        videoPipe,
        audioPipe,
      });
    };

    spawnAttempt(1);

    this.logger.info(
      `Muxed client attached via H.265 transcode (total active muxers: ${this.muxerStreams.size})`,
    );

    this.updateLivestreamStateForMuxerClients();

    const cleanup = () => {
      const entry = this.muxerStreams.get(socket);
      if (!entry) return;
      this.muxerStreams.delete(socket);
      try {
        if (entry.kind === "transcode") entry.proc.kill("SIGKILL");
      } catch (e) {
        this.logger.warn(`Transcode process kill threw during cleanup: ${e}`);
      }
      this.logger.info(
        `Muxed client detached (total active muxers: ${this.muxerStreams.size})`,
      );
      this.updateLivestreamStateForMuxerClients();
    };

    socket.on("close", cleanup);
    socket.on("error", cleanup);
  }

  /**
   * Start or stop the upstream livestream based on total consumer count
   * (TCP video clients + in-process muxer clients). Called on every
   * muxer-client attach/detach.
   */
  private async updateLivestreamStateForMuxerClients(): Promise<void> {
    const totalConsumers =
      this.connectionManager.getActiveConnectionCount() +
      this.muxerStreams.size;

    if (totalConsumers > 0 && !this.livestreamIntendedState) {
      this.livestreamIntendedState = true;
      this.lastClientActivity = Date.now();
      this.startActivityMonitoring();
      await this.ensureLivestreamState();
    }
    // Intentionally *not* stopping the livestream the moment consumer
    // count drops to 0. Scrypted's Rebroadcast plugin cycles its muxer
    // connection constantly — closes the old one, immediately opens a
    // new one for the next session. Tearing down the Eufy livestream on
    // every disconnect meant the new muxer connected to a cold pipeline,
    // and the downstream FFmpeg would hit "Unable to find sync frame in
    // rtsp prebuffer" until the next camera keyframe (2-4s).
    //
    // The activity monitor handles the genuine "everyone left" case: if
    // no data flows for ACTIVITY_TIMEOUT ms it stops the livestream
    // (lastClientActivity only advances while a consumer is reading).
  }

  /**
   * Get the port the muxed (MPEG-TS) server is listening on.
   */
  getMuxedPort(): number | undefined {
    if (this.muxedServer) {
      const address = this.muxedServer.address();
      if (address && typeof address === "object") {
        return address.port;
      }
    }
    return undefined;
  }

  /**
   * See IStreamServer.setNextTranscodeOptions. Consumed once by
   * attachTranscodeClient on the next muxed connection, then cleared.
   */
  setNextTranscodeOptions(opts: {
    bitrate?: number;
    width?: number;
    height?: number;
    fps?: number;
    profile?: string;
  }): void {
    this.nextTranscodeOptions = opts;
  }

  /**
   * Stop the TCP server
   */
  async stop(): Promise<void> {
    if (!this.isActive) {
      return;
    }

    // Clear any pending timeouts
    if (this.startStopTimeout) {
      clearTimeout(this.startStopTimeout);
      this.startStopTimeout = undefined;
    }

    // Stop activity monitoring
    this.stopActivityMonitoring();

    // Stop livestream if there are active clients
    const activeClients = this.connectionManager.getActiveConnectionCount();
    if (activeClients > 0) {
      this.livestreamIntendedState = false;
      await this.ensureLivestreamState();
    }

    // Clean up WebSocket event listeners
    if (this.eventRemover) {
      this.eventRemover();
      this.eventRemover = undefined;
      this.logger.debug("WebSocket video event listener removed");
    }

    if (this.audioEventRemover) {
      this.audioEventRemover();
      this.audioEventRemover = undefined;
      this.logger.debug("WebSocket audio event listener removed");
    }

    // Tear down all in-process muxers/transcode processes and disconnect clients
    for (const [socket, entry] of this.muxerStreams) {
      try {
        if (entry.kind === "jmuxer") {
          entry.muxer.destroy();
        } else {
          entry.proc.kill("SIGKILL");
        }
      } catch (e) {
        this.logger.warn(`Muxer/transcode cleanup threw during shutdown: ${e}`);
      }
      if (!socket.destroyed) socket.destroy();
    }
    this.muxerStreams.clear();

    // Close muxed server
    if (this.muxedServer) {
      this.muxedServer.close();
      this.muxedServer = undefined;
    }

    return new Promise((resolve) => {
      this.connectionManager.close();

      if (this.server) {
        this.server.close(() => {
          this.isActive = false;
          this.logger.info("🛑 Stream server stopped");
          this.emit("stopped");
          resolve();
        });
      } else {
        this.isActive = false;
        resolve();
      }
    });
  }

  /**
   * Stream raw H.264 video data to all connected clients
   *
   * @param data - Raw H.264 video data buffer
   * @param timestamp - Optional timestamp in milliseconds
   * @param isKeyFrame - Optional flag indicating if this is a key frame
   * @returns Promise<boolean> - True if data was successfully processed
   */
  async streamVideo(
    data: Buffer,
    timestamp?: number,
    isKeyFrame?: boolean,
  ): Promise<boolean> {
    if (!data || data.length === 0) {
      this.logger.warn("Cannot stream empty video data");
      return false;
    }

    const isHevc =
      this.videoMetadata?.videoCodec.toUpperCase() === "H265" ||
      this.videoMetadata?.videoCodec.toUpperCase() === "HEVC";

    try {
      // Validate bitstream structure (start-code rules are identical for H.264 and H.265)
      const isValid = isHevc
        ? this.h264Parser.validateHevcData(data)
        : this.h264Parser.validateH264Data(data);

      if (!isValid) {
        this.logger.warn(
          `Invalid ${isHevc ? "H.265" : "H.264"} data structure`,
        );
        return false;
      }

      // Extract NAL units and detect keyframe using codec-appropriate logic
      const nalUnits = isHevc
        ? this.h264Parser.extractNALUnitsHevc(data)
        : this.h264Parser.extractNALUnits(data);

      if (isKeyFrame === undefined) {
        isKeyFrame = nalUnits.some((nal) => nal.isKeyFrame);
      }

      // Log NAL unit information for debugging
      const nalInfo = nalUnits
        .map((nal) =>
          isHevc
            ? `${this.h264Parser.getNALTypeNameHevc(nal.type)}(${nal.type})`
            : `${this.h264Parser.getNALTypeName(nal.type)}(${nal.type})`,
        )
        .join(", ");
      this.logger.debug(
        `Processing ${isHevc ? "H.265" : "H.264"} data: ${data.length} bytes, NALs: [${nalInfo}], keyFrame: ${isKeyFrame}`,
      );

      // Cache parameter-set NAL units so new clients can decode mid-stream.
      // H.264: SPS=7, PPS=8   H.265: VPS=32, SPS=33, PPS=34
      nalUnits.forEach((nal) => {
        if (!isHevc && nal.type === 7) {
          this.cachedSPS = data;
          this.logger.debug(`Cached H.264 SPS (${data.length} bytes)`);
        } else if (!isHevc && nal.type === 8) {
          this.cachedPPS = data;
          this.logger.debug(`Cached H.264 PPS (${data.length} bytes)`);
        } else if (isHevc && nal.type === 32) {
          this.cachedVPS = data;
          this.logger.debug(`Cached H.265 VPS (${data.length} bytes)`);
        } else if (isHevc && nal.type === 33) {
          this.cachedSPS = data;
          this.logger.debug(`Cached H.265 SPS (${data.length} bytes)`);
        } else if (isHevc && nal.type === 34) {
          this.cachedPPS = data;
          this.logger.debug(`Cached H.265 PPS (${data.length} bytes)`);
        }
      });

      // Resolve any pending snapshot requests with keyframe data
      // This happens BEFORE checking if server is active, so snapshots work without TCP server
      let snapshotsHandled = false;
      if (isKeyFrame && this.snapshotResolvers.length > 0) {
        this.logger.debug(
          `Resolving ${this.snapshotResolvers.length} snapshot request(s) with keyframe data`,
        );
        const resolvers = [...this.snapshotResolvers];
        this.snapshotResolvers = [];
        resolvers.forEach(({ resolve }) => resolve(data));
        snapshotsHandled = true;
      }

      // If server is not active, we've already handled snapshot resolution above
      // Return success only if snapshots were handled, otherwise return false
      if (!this.isActive) {
        if (snapshotsHandled) {
          this.stats.framesProcessed++;
          return true; // Return true because snapshot was handled successfully
        } else {
          return false; // Server not active and no snapshots to handle
        }
      }

      // Broadcast to all connected clients
      const success = this.connectionManager.broadcast(data);

      // Update client activity timestamp when data is successfully sent
      if (success) {
        this.lastClientActivity = Date.now();
      }

      // Update statistics
      this.stats.framesProcessed++;
      this.stats.bytesTransferred += data.length;
      this.stats.lastFrameTime = new Date();

      // Log frame streaming activity
      const activeClients = this.connectionManager.getActiveConnectionCount();
      if (activeClients > 0) {
        this.logger.debug(
          `Streamed video frame: ${data.length} bytes to ${activeClients} clients`,
        );
      } else {
        this.logger.debug(
          `Processed video frame: ${data.length} bytes (no active clients)`,
        );
      }

      // Emit event
      this.emit("videoStreamed", {
        data,
        timestamp,
        isKeyFrame,
      } as StreamData);

      return true;
    } catch (error) {
      this.logger.error("Failed to stream video data:", error);
      this.emit("streamError", error);
      return false;
    }
  }

  /**
   * Get video metadata from the first received frame
   */
  getVideoMetadata(): VideoMetadata | null {
    return this.videoMetadata;
  }

  /**
   * Wait for video metadata to be received
   */
  async waitForVideoMetadata(
    timeoutMs: number = 10000,
  ): Promise<VideoMetadata> {
    if (this.videoMetadata) {
      this.logger.debug("Video metadata already available");
      return this.videoMetadata;
    }

    this.logger.debug(
      `Waiting for video metadata (timeout: ${timeoutMs}ms)...`,
    );

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.logger.warn(
          `Timeout waiting for video metadata (${timeoutMs}ms). Livestream state: ${this.livestreamActualState}, intended: ${this.livestreamIntendedState}`,
        );
        reject(
          new Error(`Timeout waiting for video metadata (${timeoutMs}ms)`),
        );
      }, timeoutMs);

      this.once("metadataReceived", (metadata) => {
        clearTimeout(timeout);
        this.logger.debug("Video metadata received successfully");
        resolve(metadata);
      });
    });
  }

  /**
   * Check if server is running
   */
  isRunning(): boolean {
    return this.isActive;
  }

  /**
   * Get server statistics
   */
  getStats(): ServerStats {
    const connectionStats = this.connectionManager.getConnectionStats();

    return {
      isActive: this.isActive,
      port: this.options.port,
      uptime: this.startTime ? Date.now() - this.startTime.getTime() : 0,
      connections: {
        active: this.connectionManager.getActiveConnectionCount(),
        total: Object.keys(connectionStats).length,
        connections: connectionStats,
      },
      streaming: {
        framesProcessed: this.stats.framesProcessed,
        bytesTransferred: this.stats.bytesTransferred,
        lastFrameTime: this.stats.lastFrameTime,
      },
    };
  }

  /**
   * Get the actual port the server is listening on
   */
  getPort(): number | undefined {
    if (this.server) {
      const address = this.server.address();
      if (address && typeof address === "object") {
        return address.port;
      }
    }
    return undefined;
  }

  /**
   * Get number of active connections
   */
  getActiveConnectionCount(): number {
    return this.connectionManager.getActiveConnectionCount();
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = {
      framesProcessed: 0,
      bytesTransferred: 0,
      lastFrameTime: null,
    };
  }

  /**
   * Capture a single snapshot frame from the stream.
   * Starts the livestream if not already running, waits for a keyframe,
   * captures the frame, and stops the stream.
   *
   * @param timeoutMs - Maximum time to wait for a snapshot (default: 15000ms)
   * @returns Promise<Buffer> - Raw H.264 keyframe data
   */
  async captureSnapshot(timeoutMs: number = 15000): Promise<Buffer> {
    this.logger.info("📸 Capturing snapshot...");

    const wasStreamRunning = this.livestreamIntendedState;

    try {
      // Start livestream if not already running or being started
      if (!this.livestreamIntendedState) {
        this.logger.debug("Starting livestream for snapshot capture");
        this.livestreamIntendedState = true;
        await this.ensureLivestreamState();
      } else {
        this.logger.debug(
          "Livestream already intended/running, waiting for keyframe",
        );
      }

      // Wait for a keyframe
      const snapshotBuffer = await new Promise<Buffer>((resolve, reject) => {
        const timeoutHandle = setTimeout(() => {
          // Remove this resolver from the list
          this.snapshotResolvers = this.snapshotResolvers.filter(
            (r) => r.resolve !== resolve,
          );
          reject(
            new Error(
              `Snapshot capture timed out after ${timeoutMs}ms - no keyframe received`,
            ),
          );
        }, timeoutMs);

        // Add resolver to the queue
        this.snapshotResolvers.push({
          resolve: (buffer: Buffer) => {
            clearTimeout(timeoutHandle);
            resolve(buffer);
          },
          reject: (error: Error) => {
            clearTimeout(timeoutHandle);
            reject(error);
          },
          timestamp: Date.now(),
        });

        this.logger.debug(
          `Waiting for next keyframe (timeout: ${timeoutMs}ms)...`,
        );
      });

      this.logger.info(
        `✅ Snapshot captured: ${snapshotBuffer.length} bytes (keyframe)`,
      );

      return snapshotBuffer;
    } finally {
      // Stop livestream if it wasn't running before
      if (!wasStreamRunning) {
        this.logger.debug(
          "Stopping livestream after snapshot capture (was not running before)",
        );
        this.livestreamIntendedState = false;
        // Don't await here to avoid blocking the snapshot return
        this.ensureLivestreamState().catch((error) => {
          this.logger.warn(
            `Failed to stop livestream after snapshot: ${error}`,
          );
        });
      }
    }
  }
}
