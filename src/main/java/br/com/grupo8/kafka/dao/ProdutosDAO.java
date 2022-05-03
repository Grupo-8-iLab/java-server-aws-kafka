package br.com.grupo8.kafka.dao;

import br.com.grupo8.kafka.models.Produto;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProdutosDAO extends CrudRepository<Produto, Integer> {
}


