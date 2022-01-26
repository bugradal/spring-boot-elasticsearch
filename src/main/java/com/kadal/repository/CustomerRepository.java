package com.kadal.repository;

import com.kadal.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository <Customer,Integer> {
}
