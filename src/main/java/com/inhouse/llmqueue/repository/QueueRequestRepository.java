package com.inhouse.llmqueue.repository;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.enums.RequestStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface QueueRequestRepository extends JpaRepository<QueueRequest, UUID> {

    Optional<QueueRequest> findByIdAndStatus(UUID id, RequestStatus status);

    @Query(value = """
            SELECT * FROM queue_requests
            WHERE model_name = :modelName
              AND status = 'queued'
              AND (scheduled_at IS NULL OR scheduled_at <= NOW())
            ORDER BY priority_weight ASC, COALESCE(scheduled_at, created_at) ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<QueueRequest> fetchNextForProcessing(@Param("modelName") String modelName,
                                               @Param("limit") int limit);

    @Query("SELECT COUNT(q) FROM QueueRequest q WHERE q.modelName = :modelName AND q.status = 'queued'")
    long countQueued(@Param("modelName") String modelName);
}
