package com.inhouse.llmqueue.repository;

import com.inhouse.llmqueue.entity.ModelConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ModelConfigRepository extends JpaRepository<ModelConfig, String> {
    List<ModelConfig> findAllByIsActiveTrue();
}
