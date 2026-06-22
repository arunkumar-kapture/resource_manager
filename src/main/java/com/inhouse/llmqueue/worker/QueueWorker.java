package com.inhouse.llmqueue.worker;

import com.inhouse.llmqueue.entity.ModelConfig;
import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.repository.ModelConfigRepository;
import com.inhouse.llmqueue.repository.QueueRequestRepository;
import com.inhouse.llmqueue.service.CapacityService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class QueueWorker {

    private final QueueRequestRepository queueRequestRepository;
    private final ModelConfigRepository modelConfigRepository;
    private final CapacityService capacityService;
    private final RequestProcessor requestProcessor;

    private static final int FETCH_SIZE = 10;

    @Scheduled(fixedDelayString = "${worker.poll-interval-ms:1000}")
    public void poll() {
        List<ModelConfig> activeModels = modelConfigRepository.findAllByIsActiveTrue();
        for (ModelConfig model : activeModels) {
            try {
                processModel(model.getModelName());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[WORKER] Interrupted while processing model {}, stopping poll cycle", model.getModelName());
                return;
            } catch (Exception e) {
                log.error("[WORKER] Error while polling queue for model {}: {}", model.getModelName(), e.getMessage());
            }
        }
    }

    public void processModel(String modelName) throws InterruptedException {
        try {
            capacityService.getActiveConfig(modelName);
        } catch (Exception e) {
            return;
        }

        // Fetch only IDs — never hold managed entities across transaction boundaries
        List<QueueRequest> requests = queueRequestRepository.fetchNextForProcessing(modelName, FETCH_SIZE);
        if (requests.isEmpty()) return;

        for (int i = 0; i < requests.size(); i++) {
            QueueRequest req = requests.get(i);

            boolean canProcess = req.getMode() == RequestMode.flex
                    ? capacityService.hasFlexCapacity(modelName)
                    : capacityService.hasBatchCapacity(modelName);

            if (!canProcess) {
                log.info("[WORKER] Model {} - capacity check failed for {} request, pausing - {} requests remain queued",
                        modelName, req.getMode(), requests.size() - i);
                return;
            }

            // Pass only the UUID — RequestProcessor opens its own transaction and fetches fresh
            requestProcessor.process(req.getId());

            if (i < requests.size() - 1) {
                Thread.sleep(1000);
            }
        }
    }
}
