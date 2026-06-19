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

    @Query("""
            SELECT a FROM AuditLog a
            WHERE (:modelName IS NULL OR a.modelName = :modelName)
              AND (:mode IS NULL OR CAST(a.mode AS string) = :mode)
              AND (:status IS NULL OR CAST(a.status AS string) = :status)
              AND (:source IS NULL OR CAST(a.source AS string) = :source)
              AND (:from IS NULL OR a.createdAt >= :from)
              AND (:to IS NULL OR a.createdAt <= :to)
            ORDER BY a.createdAt DESC
            """)
    Page<AuditLog> search(@Param("modelName") String modelName,
                          @Param("mode") String mode,
                          @Param("status") String status,
                          @Param("source") String source,
                          @Param("from") OffsetDateTime from,
                          @Param("to") OffsetDateTime to,
                          Pageable pageable);
}
