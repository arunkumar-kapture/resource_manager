package com.inhouse.llmqueue.repository;

import com.inhouse.llmqueue.entity.AuditLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;

@Repository
public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {

    @Query(value = """
            SELECT * FROM audit_log
            WHERE (:modelName IS NULL OR model_name = :modelName)
              AND (:mode     IS NULL OR mode::text   = :mode)
              AND (:status   IS NULL OR status::text = :status)
              AND (:source   IS NULL OR source::text = :source)
              AND (:from::timestamptz IS NULL OR created_at >= :from::timestamptz)
              AND (:to::timestamptz   IS NULL OR created_at <= :to::timestamptz)
            ORDER BY created_at DESC
            """,
            countQuery = """
            SELECT COUNT(*) FROM audit_log
            WHERE (:modelName IS NULL OR model_name = :modelName)
              AND (:mode     IS NULL OR mode::text   = :mode)
              AND (:status   IS NULL OR status::text = :status)
              AND (:source   IS NULL OR source::text = :source)
              AND (:from::timestamptz IS NULL OR created_at >= :from::timestamptz)
              AND (:to::timestamptz   IS NULL OR created_at <= :to::timestamptz)
            """,
            nativeQuery = true)
    Page<AuditLog> search(@Param("modelName") String modelName,
                          @Param("mode") String mode,
                          @Param("status") String status,
                          @Param("source") String source,
                          @Param("from") OffsetDateTime from,
                          @Param("to") OffsetDateTime to,
                          Pageable pageable);
}
