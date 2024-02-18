package io.airbyte.cdk.integrations.destination_async


import io.airbyte.cdk.integrations.destination.async.AirbyteFileUtils
import io.airbyte.cdk.integrations.destination.async.DetectStreamToFlush
import io.airbyte.cdk.integrations.destination.async.RunningFlushWorkers
import io.airbyte.cdk.integrations.destination.async.buffers.BufferDequeue
import io.airbyte.cdk.integrations.destination.async.buffers.StreamAwareQueue
import io.airbyte.cdk.integrations.destination.async.function.DestinationFlushFunction
import io.airbyte.cdk.integrations.destination.async.state.FlushFailure
import io.airbyte.cdk.integrations.destination.async.state.GlobalAsyncStateManager
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.produce
import org.slf4j.LoggerFactory


@OptIn(ExperimentalCoroutinesApi::class)
class FlushWorkersV2(
    private val bufferDequeue: BufferDequeue,
    private val flusher: DestinationFlushFunction,
    private val outputRecordCollector: Consumer<AirbyteMessage?>,
    private val flushFailure: FlushFailure,
    private val stateManager: GlobalAsyncStateManager,
    private val dispatcher: CoroutineDispatcher
) :
    AutoCloseable {
    private val logger = LoggerFactory.getLogger(FlushWorkersV2::class.java)
    private val isClosing = AtomicBoolean(false)
    private val runningFlushWorkers = RunningFlushWorkers()
    private val detectStreamToFlush =
        DetectStreamToFlush(bufferDequeue, runningFlushWorkers, isClosing, flusher)
    private val supervisor = CoroutineScope(Dispatchers.IO)
    private var isClosed = false

    private fun detectingStreamsToFlush() = supervisor.produce {
        while (isActive && !isClosed) {
            val nextStreamToFlush = detectStreamToFlush.nextStreamToFlush
            if (nextStreamToFlush.isPresent)
                send(nextStreamToFlush.get())
            delay(1.seconds)
        }
    }

    fun start() {
        logger.info("Start async buffer supervisor")
        supervisor.launch {
            for (stream in detectingStreamsToFlush()) {
                val flushWorkerId = UUID.randomUUID()
                runningFlushWorkers.trackFlushWorker(stream, flushWorkerId)
                flush(stream, flushWorkerId)
            }
        }
    }

    private fun flush(desc: StreamDescriptor, flushWorkerId: UUID) = supervisor.launch(dispatcher) {
        try {
            logger.info(
                "Flush Worker ({}) -- Attempting to read from queue namespace: {}, stream: {}.",
                flushWorkerId.readable(),
                desc.namespace,
                desc.name,
            )
            bufferDequeue.take(desc, flusher.optimalBatchSizeBytes).use { batch ->
                runningFlushWorkers.registerBatchSize(desc, flushWorkerId, batch.sizeInBytes)
                val stateIdToCount: Map<Long?, Long?> = batch.data
                    .map { it.stateId }
                    .groupingBy { it }
                    .eachCount()
                    .mapValues { it.value.toLong() }

                logger.info(
                    "Flush Worker ({}) -- Batch contains: {} records, {} bytes.",
                    flushWorkerId.readable(),
                    batch.data.size,
                    AirbyteFileUtils.byteCountToDisplaySize(batch.sizeInBytes),
                )
                flusher.flush(
                    desc,
                    batch.data.stream().map(StreamAwareQueue.MessageWithMeta::message),
                )
                batch.flushStates(stateIdToCount, outputRecordCollector)
            }
            logger.info(
                "Flush Worker ({}) -- Worker finished flushing. Current queue size: {}",
                flushWorkerId.readable(),
                bufferDequeue.getQueueSizeInRecords(desc).orElseThrow(),
            )
        } catch (e: Exception) {
            logger.error("Flush Worker (${flushWorkerId.readable()}) -- flush worker error: $e")
            flushFailure.propagateException(e)
            throw RuntimeException(e)
        } finally {
            runningFlushWorkers.completeFlushWorker(desc, flushWorkerId)
        }
    }

    @Throws(java.lang.Exception::class)
    override fun close(): Unit = runBlocking {
        logger.info("Closing flush workers -- Waiting for all buffers to flush")
        isClosing.set(true)
        // wait for all buffers to be flushed.
        while (true) {
            val streamDescriptorToRemainingRecords =
                bufferDequeue.bufferedStreams.associateWith { desc ->
                    bufferDequeue.getQueueSizeInRecords(desc).orElseThrow()
                }

            val anyRecordsLeft = streamDescriptorToRemainingRecords.values.any { size -> size > 0 }
            if (!anyRecordsLeft) {
                logger.info("Closing flush workers -- All buffers flushed")
                isClosed = true
                break
            }

            val workerInfo = streamDescriptorToRemainingRecords.entries.filter { it.value > 0 }
                .fold("REMAINING_BUFFERS_INFO -- ") { acc, entry ->
                    acc + "${entry.key.namespace}.${entry.key.name}: ${entry.value} records "
                }
            logger.info(workerInfo)

            logger.info("Closing flush workers -- Waiting for all streams to flush.")
            delay(5.seconds)
        }
        // before shutting down the supervisor, flush all state.
        stateManager.flushStates(outputRecordCollector)

        withTimeoutOrNull(5.minutes) {
            logger.info("Closing flush workers -- Waiting for all flush workers to finish")
            supervisor.coroutineContext[Job]?.children?.forEach { it.join() }
            logger.info("Closing flush workers -- All flush workers finished")
        }

        supervisor.run {
            logger.info("Closing flush workers -- Cancelling supervisor")
            cancel()
            logger.info("Closing flush workers -- Supervisor cancelled")
        }
    }

    private fun UUID.readable() = toString().substring(0, 5)
}
