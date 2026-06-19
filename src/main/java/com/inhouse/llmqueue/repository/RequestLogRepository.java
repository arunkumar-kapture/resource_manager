package com.inhouse.llmqueue.repository;

import com.inhouse.llmqueue.entity.RequestLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;

@Repository
public interface RequestLogRepository extends JpaRepository<RequestLog, Long> {

    @Query("SELECT COUNT(r) FROM RequestLog r WHERE r.modelName = :modelName AND r.createdAt >= :since")
    long countByModelNameAndCreatedAtAfter(@Param("modelName") String modelName,
                                           @Param("since") OffsetDateTime since);

    @Modifying
    @Query("DELETE FROM RequestLog r WHERE r.createdAt < :before")
    int deleteOlderThan(@Param("before") OffsetDateTime before);
}
