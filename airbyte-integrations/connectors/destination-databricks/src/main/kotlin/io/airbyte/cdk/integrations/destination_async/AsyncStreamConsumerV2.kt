package io.airbyte.cdk.integrations.destination_async

import com.google.common.base.Preconditions
import com.google.common.base.Strings
import io.airbyte.cdk.integrations.base.SerializedAirbyteMessageConsumer
import io.airbyte.cdk.integrations.destination.StreamSyncSummary
import io.airbyte.cdk.integrations.destination.async.StreamDescriptorUtils
import io.airbyte.cdk.integrations.destination.async.buffers.BufferManager
import io.airbyte.cdk.integrations.destination.async.deser.DeserializationUtil
import io.airbyte.cdk.integrations.destination.async.deser.StreamAwareDataTransformer
import io.airbyte.cdk.integrations.destination.async.function.DestinationFlushFunction
import io.airbyte.cdk.integrations.destination.async.partial_messages.PartialAirbyteMessage
import io.airbyte.cdk.integrations.destination.async.state.FlushFailure
import io.airbyte.cdk.integrations.destination.buffered_stream_consumer.OnCloseFunction
import io.airbyte.cdk.integrations.destination.buffered_stream_consumer.OnStartFunction
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.StreamDescriptor
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import kotlin.jvm.optionals.getOrNull
import kotlinx.coroutines.CoroutineDispatcher
import org.slf4j.LoggerFactory


class AsyncStreamConsumerV2(
    private val outputRecordCollector: Consumer<AirbyteMessage?>,
    private val onStart: OnStartFunction,
    private val onClose: OnCloseFunction,
    private val flusher: DestinationFlushFunction,
    private val flushFailure: FlushFailure = FlushFailure(),
    private val catalog: ConfiguredAirbyteCatalog,
    private val bufferManager: BufferManager,
    private val defaultNamespace: Optional<String>,
    private val dataTransformer: StreamAwareDataTransformer,
    private val deserializationUtil: DeserializationUtil,
    private val dispatcher: CoroutineDispatcher,
) : SerializedAirbyteMessageConsumer {
    private val logger = LoggerFactory.getLogger(AsyncStreamConsumerV2::class.java)
    private val streamNames = StreamDescriptorUtils.fromConfiguredCatalog(catalog)
    private val bufferEnqueue = bufferManager.bufferEnqueue
    private var flushWorkers: FlushWorkersV2 = FlushWorkersV2(
        bufferManager.bufferDequeue,
        flusher,
        outputRecordCollector,
        flushFailure,
        bufferManager.stateManager,
        dispatcher,
    )

    // Note that this map will only be populated for streams with nonzero records.
    private val recordCounts = ConcurrentHashMap<StreamDescriptor, AtomicLong>()
    private var hasStarted = false
    private var hasClosed = false
    private var hasFailed = false

    // This is to account for the references when deserialization to a PartialAirbyteMessage. The
    // calculation is as follows:
    // PartialAirbyteMessage (4) + Max( PartialRecordMessage(4), PartialStateMessage(6)) with
    // PartialStateMessage being larger with more nested objects within it. Using 8 bytes as we assumed
    // a 64 bit JVM.
    private val PARTIAL_DESERIALIZE_REF_BYTES = 10 * 8

    @Throws(Exception::class)
    override fun start() {
        Preconditions.checkState(!hasStarted, "Consumer has already been started.")
        hasStarted = true
        flushWorkers.start()
        logger.info("{} started.", AsyncStreamConsumerV2::class.java)
        onStart.call()
    }

    @Throws(Exception::class)
    override fun accept(messageString: String, sizeInBytes: Int) {
        Preconditions.checkState(hasStarted, "Cannot accept records until consumer has started")
        propagateFlushWorkerExceptionIfPresent()
        /*
         * intentionally putting extractStream outside the buffer manager so that if in the future we want
         * to try to use a thread pool to partially deserialize to get record type and stream name, we can
         * do it without touching buffer manager.
         */
        val message =
            deserializationUtil.deserializeAirbyteMessage(
                messageString,
                dataTransformer,
            )
        if (AirbyteMessage.Type.RECORD == message.type) {
            if (Strings.isNullOrEmpty(message.record?.namespace)) {
                message.record?.namespace = defaultNamespace.getOrNull()
            }
            validateRecord(message)

            message.record?.streamDescriptor?.let { getRecordCounter(it).incrementAndGet() }
        }
        bufferEnqueue.addRecord(
            message,
            sizeInBytes + PARTIAL_DESERIALIZE_REF_BYTES,
            defaultNamespace,
        )
    }

    @Throws(Exception::class)
    override fun close() {
        Preconditions.checkState(hasStarted, "Cannot close; has not started.")
        Preconditions.checkState(!hasClosed, "Has already closed.")
        hasClosed = true

        // assume closing upload workers will flush all accepted records.
        // we need to close the workers before closing the bufferManagers (and underlying buffers)
        // or we risk in-memory data.
        flushWorkers.close()
        bufferManager.close()
        val streamSyncSummaries = streamNames.associateWith { streamDescriptor ->
            StreamSyncSummary(Optional.of(getRecordCounter(streamDescriptor).get()))
        }

        onClose.accept(hasFailed, streamSyncSummaries)

        // as this throws an exception, we need to be after all others close functions.
        propagateFlushWorkerExceptionIfPresent()
        logger.info("{} closed", AsyncStreamConsumerV2::class.java)
    }

    private fun getRecordCounter(streamDescriptor: StreamDescriptor) = recordCounts.computeIfAbsent(
        streamDescriptor,
    ) {
        AtomicLong()
    }

    @Throws(Exception::class)
    private fun propagateFlushWorkerExceptionIfPresent() {
        if (flushFailure.isFailed()) {
            hasFailed = true
            throw flushFailure.exception
        }
    }

    private fun validateRecord(message: PartialAirbyteMessage) {
        val streamDescriptor = StreamDescriptor()
            .withNamespace(message.record?.namespace)
            .withName(message.record?.stream)
        // if stream is not part of list of streams to sync to then throw invalid stream exception
        if (!streamNames.contains(streamDescriptor)) {
            throwUnrecognizedStream(catalog, message)
        }
    }

    private fun throwUnrecognizedStream(
        catalog: ConfiguredAirbyteCatalog,
        message: PartialAirbyteMessage
    ) {
        throw IllegalArgumentException(
            String.format(
                "Message contained record from a stream that was not in the catalog. \ncatalog: %s , \nmessage: %s",
                Jsons.serialize(catalog), Jsons.serialize(message),
            ),
        )
    }
}
