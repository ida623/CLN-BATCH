package cub.lip.bch.cln.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import cub.lip.bch.cln.entity.ClnCustomerIdTempEntity;

@Repository("CLN_CUSTOMER_ID_TEMP")
public interface ClnCustomerIdTempRepository extends JpaRepository<ClnCustomerIdTempEntity, String> {
}